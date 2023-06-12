package com.github.spring.esdata.loader.core;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Loader that use Spring Data's {@link ElasticsearchOperations} to load data into Elasticsearch.
 *
 * @author tinesoft
 */
public class SpringEsDataLoader implements EsDataLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpringEsDataLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ElasticsearchOperations esOperations;

  /**
   * .
   * Data loader that use Spring's {@link ElasticsearchOperations} to load data into Elasticsearch
   *
   * @param esOperations the {@link ElasticsearchOperations}
   */
  @Autowired
  public SpringEsDataLoader(final ElasticsearchOperations esOperations) {
    this.esOperations = esOperations;
  }

  /**
   * Deletes data from Elasticsearch using provided class to retrieve related index.
   *
   * @param esEntityClass the data to load
   */
  @Override
  public void delete(final Class<?> esEntityClass) {
    LOGGER.debug("Dropping data in Index '{}'...", esEntityClass.getSimpleName());
    final IndexOperations indexOps = this.esOperations.indexOps(esEntityClass);
    indexOps.delete();
    indexOps.create();
    indexOps.putMapping();
    indexOps.refresh();

  }

  /**
   * Loads given data into Elasticsearch. Target indices are dropped and recreated before data are inserted in bulk.
   *
   * @param d the data to load
   */
  @Override
  public void load(final IndexData d) {

    // first recreate the index
    LOGGER.debug("Recreating Index for '{}'...", d.getEsEntityClass().getSimpleName());
    final IndexOperations indexOps = this.esOperations.indexOps(d.esEntityClass);
    indexOps.delete();
    indexOps.create();
    indexOps.putMapping();
    indexOps.refresh();

    final ElasticsearchPersistentEntity<?> esEntityInfo = this.esOperations.getElasticsearchConverter().getMappingContext().getPersistentEntity(d.esEntityClass);
//    final ElasticsearchPersistentEntity<?> esEntityInfo = this.esOperations.getPersistentEntityFor(d.esEntityClass);

    LOGGER.debug("Inserting data in Index of '{}'. Please wait...", d.getEsEntityClass().getSimpleName());

    // then insert data into it
    try (InputStream is = this.getClass().getResourceAsStream(d.getLocation()); //
         BufferedReader br = new BufferedReader(
           new InputStreamReader(d.gzipped ? new GZIPInputStream(is) : is, StandardCharsets.UTF_8))) {

      final EsDataFormat format = getEsDataFormat(br, d.format);

      final List<IndexQuery> indexQueries = (format == EsDataFormat.DUMP ? br.lines() : this.toJsonArrayStream(br)) // each item represent a document to be indexed
        .parallel()// let's speed things up a lil'bit :)
        .peek((l) -> LOGGER.debug("Preparing IndexQuery for line: '{}'", l))//
        .map(json -> getIndexQuery(json, esEntityInfo.getIndexCoordinates().getIndexName(), format))//
        .skip(d.nbSkipItems)//
        .limit(d.nbMaxItems)//
        .collect(Collectors.toList());

      if (indexQueries.isEmpty()) {
        LOGGER.warn("There are no data to load from file at '{}'. Please review its content", d.location);
        return;
      }

      this.esOperations.bulkIndex(indexQueries, d.esEntityClass);
      indexOps.refresh();

      LOGGER.debug("Insertion successfully done");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Builds an {@link IndexQuery} based on the provided JSON content, representing the data to be inserted into ES.
   *
   * @param jsonContent the data to be inserted, expressed as JSON
   * @param indexName   the name of the target index
   * @param format      the format of data to index
   * @return the {@link IndexQuery} built from the parsed JSON
   */
  private static IndexQuery getIndexQuery(final String jsonContent, final String indexName, final EsDataFormat format) {

    try {
      String id = null;
      String source = jsonContent;

      if (format == EsDataFormat.DUMP) {
        final JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonContent);
        id = jsonNode.get("_id").textValue();
        source = jsonNode.get("_source").toString();
      }

      return new IndexQueryBuilder()//
        .withId(id)//
        .withIndex(indexName)//
        .withSource(source)//
        .build();

    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns the format of data to load (if specified) or try to auto-detect it from the JSOC content.
   *
   * @param br           the reader to read the JSON content
   * @param esDataFormat the specified format
   * @return the es data format
   * @throws IOException
   */
  private static EsDataFormat getEsDataFormat(final BufferedReader br, final EsDataFormat esDataFormat) throws IOException {
    if (esDataFormat != EsDataFormat.UNKNOWN)
      return esDataFormat;

    // try to detect the format from the content of the json content
    br.mark(1024);// mark the stream at the beginning, to later rewind to that position
    final String firstLine = br.lines().filter(l -> !l.isEmpty()).findFirst().orElse("");
    br.reset();// rewind the stream to the last mark

    if (firstLine.contains("_index") && firstLine.contains("_source"))
      return EsDataFormat.DUMP;
    else if (firstLine.startsWith("["))
      return EsDataFormat.MANUAL;
    else
      throw new IllegalArgumentException("Could not auto-detect the format of data to load");
  }

  /**
   * Converts the given {@link Reader} (that represents a array of JSON objects)  into a {@link Stream}.
   *
   * @param reader the reader
   * @return a {@link Stream} of JSON objects as String
   * @throws IOException
   */
  private Stream<String> toJsonArrayStream(final Reader reader) throws IOException {
    final JsonParser jsonParser = OBJECT_MAPPER.getFactory().createParser(reader);
    if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalStateException("Not a valid EsDataFormat.MANUAL format. Expected an array");
    }
    final Iterator<String> iterator = new Iterator<String>() {
      String nextObject = null;

      @Override
      public boolean hasNext() {
        if (this.nextObject != null) {
          return true;
        } else {
          try {
            this.nextObject = this.readObject();
            return (this.nextObject != null);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }

      @Override
      public String next() {
        if (this.nextObject != null || this.hasNext()) {
          final String object = this.nextObject;
          this.nextObject = null;
          return object;
        } else {
          throw new NoSuchElementException();
        }
      }

      String readObject() throws IOException {
        final JsonToken nextToken = jsonParser.nextToken();
        if (nextToken != null && nextToken != JsonToken.END_ARRAY) {
          return OBJECT_MAPPER.readTree(jsonParser).toString();
        }

        jsonParser.close();//no more objects in the array, close the stream
        return null;
      }
    };
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
      iterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
  }
}
