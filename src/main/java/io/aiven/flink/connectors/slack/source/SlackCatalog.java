package io.aiven.flink.connectors.slack.source;

import static io.aiven.flink.connectors.slack.Constants.BOT_TOKEN;
import static io.aiven.flink.connectors.slack.Constants.CHANNEL_ID;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

public class SlackCatalog implements Catalog {

  private static final String DEFAULT_DATABASE = "slack";
  private MethodsClient client;
  private ReadableConfig config;
  private final String name;

  public SlackCatalog(String name, ReadableConfig config) {
    this.name = name;
    this.config = config;
  }

  @Override
  public void open() throws CatalogException {
    client = Slack.getInstance().methods(BOT_TOKEN.key());
  }

  @Override
  public void close() throws CatalogException {}

  @Override
  public String getDefaultDatabase() throws CatalogException {
    return DEFAULT_DATABASE;
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return List.of(DEFAULT_DATABASE);
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(name, databaseName);
    }

    return new CatalogDatabaseImpl(new HashMap<>(), "Lists all folders as source tables");
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    return getDefaultDatabase().equals(databaseName);
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    if (databaseExists(name)) {
      if (ignoreIfExists) {
        return;
      }

      throw new DatabaseAlreadyExistException(name, name);
    }

    throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    if (!databaseExists(name)) {
      if (ignoreIfNotExists) {
        return;
      }

      throw new DatabaseNotExistException(name, name);
    }

    throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(name)) {
      if (ignoreIfNotExists) {
        return;
      }

      throw new DatabaseNotExistException(name, name);
    }

    throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(name, tablePath);
    }

    Schema.Builder schemaBuilder = Schema.newBuilder();
    SlackMessageMetadata.MAP.forEach(
        (key, value) -> schemaBuilder.columnByMetadata(key, value.getDataType()));

    final Map<String, String> sourceOptions = new HashMap<>();
    sourceOptions.put("token", BOT_TOKEN.key());
    sourceOptions.put("channel_id", CHANNEL_ID.key());

    return CatalogTable.of(schemaBuilder.build(), null, Collections.emptyList(), sourceOptions);
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    return false;
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {}

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {}

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {}

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {}

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return null;
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          CatalogException {
    return null;
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    return null;
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return false;
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {}

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {}

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {}

  @Override
  public List<String> listFunctions(String dbName)
      throws DatabaseNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    return null;
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {}

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {}

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {}

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return null;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {}

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {}

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {}

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {}
}
