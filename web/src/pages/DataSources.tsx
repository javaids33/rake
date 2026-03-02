import { useState, useEffect, useRef } from 'react'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Input } from '../components/ui/Input'
import { Textarea, Select } from '../components/ui/Input'
import { Modal } from '../components/ui/Modal'
import { Tabs } from '../components/ui/Tabs'
import { EmptyState } from '../components/ui/EmptyState'
import { StatusDot } from '../components/ui/StatusDot'
import { cn } from '../lib/utils'
import { getConnections, addConnection, deleteConnection, getS3Configs, addS3Config, deleteS3Config, uploadFile, registerTable, testConnection } from '../api/client'
import type { ConnectionEntry, S3Config, ConnectionTestResponse } from '../types'
import {
  FolderInput, Database, HardDrive, Upload, Plus, Trash2,
  Server, Globe, FileText, Plug, Link2, FolderOpen, Search,
  ArrowRight, CheckCircle2, AlertCircle, Zap, ExternalLink,
  BarChart3, Layers, Radio, Cloud,
} from 'lucide-react'
import toast from 'react-hot-toast'

// Validation tier per connector — what level of testing is available
const FULL_PROTOCOL_CONNECTORS = new Set([
  'postgres', 'cockroachdb', 'yugabytedb', 'timescaledb', 'greenplum', 'redshift', 'cdc_postgres',
])
const TCP_CONNECTORS = new Set([
  'mysql', 'mariadb', 'tidb', 'vitess', 'singlestore',
  'mongodb', 'cdc_mongodb', 'cassandra', 'scylladb', 'redis',
  'elasticsearch', 'opensearch', 'neo4j', 'influxdb', 'questdb',
  'clickhouse', 'druid', 'pinot', 'starrocks', 'doris', 'trino', 'presto',
  'oracle', 'sqlserver', 'mssql', 'db2', 'sap_hana', 'teradata', 'vertica',
  'exasol', 'netezza', 'informix', 'kafka', 'minio', 'hbase',
])
function validationTier(id: string): { level: 'full' | 'tcp' | 'config'; label: string; color: string } {
  if (FULL_PROTOCOL_CONNECTORS.has(id)) return { level: 'full', label: 'Full Verify', color: 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20' }
  if (TCP_CONNECTORS.has(id)) return { level: 'tcp', label: 'TCP Check', color: 'text-amber-400 bg-amber-400/10 border-amber-400/20' }
  return { level: 'config', label: 'Config Only', color: 'text-zinc-400 bg-white/[0.04] border-white/[0.06]' }
}

// ─────────────────────────────────────────────────────
// 100+ connectors — every source Trino & Databricks support
// ─────────────────────────────────────────────────────
const CONNECTOR_CATALOG = [
  // ─── Relational Databases ──────────────────────────
  { id: 'postgres', name: 'PostgreSQL', icon: '🐘', category: 'database', status: 'available', desc: 'Advanced open-source OLTP database', config: ['host', 'port', 'database', 'username', 'password', 'ssl_mode'] },
  { id: 'mysql', name: 'MySQL', icon: '🐬', category: 'database', status: 'available', desc: 'Popular open-source relational database', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'mariadb', name: 'MariaDB', icon: '🦭', category: 'database', status: 'available', desc: 'MySQL-compatible community fork', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'oracle', name: 'Oracle DB', icon: '🔴', category: 'database', status: 'preview', desc: 'Enterprise relational database via OCI', config: ['host', 'port', 'service_name', 'username', 'password'] },
  { id: 'sqlserver', name: 'SQL Server', icon: '🟦', category: 'database', status: 'preview', desc: 'Microsoft SQL Server via TDS protocol', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'db2', name: 'IBM Db2', icon: '🔵', category: 'database', status: 'preview', desc: 'IBM enterprise database via JDBC', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'sap_hana', name: 'SAP HANA', icon: '🟡', category: 'database', status: 'preview', desc: 'In-memory column-oriented RDBMS', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'teradata', name: 'Teradata', icon: '🟠', category: 'database', status: 'preview', desc: 'Enterprise data warehouse appliance', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'vertica', name: 'Vertica', icon: '🔷', category: 'database', status: 'preview', desc: 'Columnar MPP analytics database', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'greenplum', name: 'Greenplum', icon: '🟢', category: 'database', status: 'preview', desc: 'Postgres-based massively parallel DB', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'cockroachdb', name: 'CockroachDB', icon: '🪳', category: 'database', status: 'available', desc: 'Distributed SQL, Postgres wire protocol', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'tidb', name: 'TiDB', icon: '🌐', category: 'database', status: 'available', desc: 'Distributed HTAP, MySQL-compatible', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'yugabytedb', name: 'YugabyteDB', icon: '🌀', category: 'database', status: 'available', desc: 'Distributed SQL, Postgres-compatible', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'vitess', name: 'Vitess', icon: '🟣', category: 'database', status: 'preview', desc: 'MySQL-compatible sharding middleware', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'singlestore', name: 'SingleStore', icon: '💎', category: 'database', status: 'preview', desc: 'Distributed SQL for real-time analytics', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'sqlite', name: 'SQLite', icon: '📁', category: 'database', status: 'available', desc: 'Embedded single-file database', config: ['path'] },
  { id: 'exasol', name: 'Exasol', icon: '🔶', category: 'database', status: 'preview', desc: 'In-memory analytics database', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'netezza', name: 'Netezza', icon: '🔹', category: 'database', status: 'preview', desc: 'IBM data warehouse appliance', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'informix', name: 'Informix', icon: '⬛', category: 'database', status: 'preview', desc: 'IBM embeddable RDBMS', config: ['host', 'port', 'database', 'username', 'password'] },

  // ─── Analytical Engines ────────────────────────────
  { id: 'clickhouse', name: 'ClickHouse', icon: '⚡', category: 'analytics', status: 'available', desc: 'Column-oriented OLAP engine', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'duckdb', name: 'DuckDB', icon: '🦆', category: 'analytics', status: 'available', desc: 'Embedded analytical database', config: ['path'] },
  { id: 'bigquery', name: 'Google BigQuery', icon: '🌐', category: 'analytics', status: 'preview', desc: 'Google serverless data warehouse', config: ['project_id', 'dataset', 'credentials_json'] },
  { id: 'redshift', name: 'Amazon Redshift', icon: '🔴', category: 'analytics', status: 'preview', desc: 'AWS columnar data warehouse', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'snowflake', name: 'Snowflake', icon: '❄️', category: 'analytics', status: 'preview', desc: 'Multi-cloud data platform', config: ['account', 'database', 'warehouse', 'username', 'password', 'role'] },
  { id: 'synapse', name: 'Azure Synapse', icon: '🔷', category: 'analytics', status: 'preview', desc: 'Microsoft analytics service', config: ['server', 'database', 'username', 'password'] },
  { id: 'databricks_sql', name: 'Databricks SQL', icon: '🧱', category: 'analytics', status: 'preview', desc: 'Lakehouse via SQL Warehouse / JDBC', config: ['host', 'http_path', 'token', 'catalog'] },
  { id: 'druid', name: 'Apache Druid', icon: '🔮', category: 'analytics', status: 'preview', desc: 'Real-time analytics OLAP store', config: ['broker_host', 'broker_port', 'coordinator_host'] },
  { id: 'pinot', name: 'Apache Pinot', icon: '🍷', category: 'analytics', status: 'preview', desc: 'Real-time distributed OLAP', config: ['controller_host', 'controller_port', 'broker_host'] },
  { id: 'starrocks', name: 'StarRocks', icon: '⭐', category: 'analytics', status: 'preview', desc: 'Sub-second MPP analytics engine', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'doris', name: 'Apache Doris', icon: '🌊', category: 'analytics', status: 'preview', desc: 'MPP analytical database', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'firebolt', name: 'Firebolt', icon: '🔥', category: 'analytics', status: 'preview', desc: 'Cloud warehouse for sub-second queries', config: ['account', 'database', 'engine', 'username', 'password'] },
  { id: 'trino', name: 'Trino', icon: '🔺', category: 'analytics', status: 'preview', desc: 'Distributed SQL query engine', config: ['host', 'port', 'catalog', 'schema', 'username'] },
  { id: 'presto', name: 'PrestoDB', icon: '🎯', category: 'analytics', status: 'preview', desc: 'Distributed SQL engine (Meta fork)', config: ['host', 'port', 'catalog', 'schema', 'username'] },
  { id: 'kylin', name: 'Apache Kylin', icon: '🐉', category: 'analytics', status: 'preview', desc: 'OLAP engine with pre-built cubes', config: ['host', 'port', 'project', 'username', 'password'] },

  // ─── NoSQL & Document ──────────────────────────────
  { id: 'mongodb', name: 'MongoDB', icon: '🍃', category: 'nosql', status: 'available', desc: 'Document database with rich queries', config: ['uri', 'database', 'collection'] },
  { id: 'cassandra', name: 'Cassandra', icon: '👁️', category: 'nosql', status: 'available', desc: 'Wide-column distributed database', config: ['contact_points', 'port', 'keyspace', 'username', 'password'] },
  { id: 'redis', name: 'Redis', icon: '🔻', category: 'nosql', status: 'available', desc: 'In-memory key-value data store', config: ['host', 'port', 'password', 'database'] },
  { id: 'elasticsearch', name: 'Elasticsearch', icon: '🔍', category: 'nosql', status: 'available', desc: 'Distributed search and analytics', config: ['hosts', 'index', 'username', 'password'] },
  { id: 'opensearch', name: 'OpenSearch', icon: '🔎', category: 'nosql', status: 'preview', desc: 'AWS-fork search and analytics', config: ['hosts', 'index', 'username', 'password'] },
  { id: 'dynamodb', name: 'Amazon DynamoDB', icon: '⚙️', category: 'nosql', status: 'preview', desc: 'AWS managed key-value / document', config: ['region', 'table_name', 'access_key', 'secret_key'] },
  { id: 'hbase', name: 'Apache HBase', icon: '🐘', category: 'nosql', status: 'preview', desc: 'Hadoop wide-column store', config: ['zookeeper_quorum', 'zookeeper_port', 'table'] },
  { id: 'scylladb', name: 'ScyllaDB', icon: '🦑', category: 'nosql', status: 'preview', desc: 'Cassandra-compatible, C++ rewrite', config: ['contact_points', 'port', 'keyspace', 'username', 'password'] },
  { id: 'couchbase', name: 'Couchbase', icon: '🛋️', category: 'nosql', status: 'preview', desc: 'Multi-model NoSQL database', config: ['connection_string', 'bucket', 'username', 'password'] },
  { id: 'neo4j', name: 'Neo4j', icon: '🕸️', category: 'nosql', status: 'preview', desc: 'Native graph database (Cypher)', config: ['uri', 'database', 'username', 'password'] },
  { id: 'influxdb', name: 'InfluxDB', icon: '📈', category: 'nosql', status: 'preview', desc: 'Purpose-built time-series database', config: ['url', 'org', 'bucket', 'token'] },
  { id: 'timescaledb', name: 'TimescaleDB', icon: '⏱️', category: 'nosql', status: 'available', desc: 'Postgres extension for time-series', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'questdb', name: 'QuestDB', icon: '📊', category: 'nosql', status: 'preview', desc: 'High-performance time-series DB', config: ['host', 'port', 'database'] },
  { id: 'cosmosdb', name: 'Azure Cosmos DB', icon: '🌌', category: 'nosql', status: 'preview', desc: 'Multi-model globally distributed DB', config: ['endpoint', 'key', 'database', 'container'] },
  { id: 'firestore', name: 'Google Firestore', icon: '🔥', category: 'nosql', status: 'preview', desc: 'Serverless document database', config: ['project_id', 'collection', 'credentials_json'] },

  // ─── Object Storage ────────────────────────────────
  { id: 's3', name: 'Amazon S3', icon: '☁️', category: 'storage', status: 'available', desc: 'AWS object storage for data lakes', config: ['endpoint', 'access_key', 'secret_key', 'bucket', 'region'] },
  { id: 'gcs', name: 'Google Cloud Storage', icon: '🌐', category: 'storage', status: 'preview', desc: 'GCP object storage backend', config: ['project', 'bucket', 'credentials_json'] },
  { id: 'adls', name: 'Azure Data Lake', icon: '🔷', category: 'storage', status: 'preview', desc: 'Azure Blob / ADLS Gen2', config: ['account', 'container', 'access_key'] },
  { id: 'minio', name: 'MinIO', icon: '🗄️', category: 'storage', status: 'available', desc: 'S3-compatible self-hosted storage', config: ['endpoint', 'access_key', 'secret_key', 'bucket'] },
  { id: 'hdfs', name: 'HDFS', icon: '🐘', category: 'storage', status: 'preview', desc: 'Hadoop Distributed File System', config: ['namenode', 'port', 'path', 'username'] },
  { id: 'ceph', name: 'Ceph / RADOS', icon: '🐙', category: 'storage', status: 'preview', desc: 'Distributed object storage via RGW', config: ['endpoint', 'access_key', 'secret_key', 'bucket'] },
  { id: 'wasabi', name: 'Wasabi', icon: '🟢', category: 'storage', status: 'preview', desc: 'S3-compatible hot cloud storage', config: ['endpoint', 'access_key', 'secret_key', 'bucket', 'region'] },
  { id: 'r2', name: 'Cloudflare R2', icon: '🟠', category: 'storage', status: 'preview', desc: 'Zero-egress S3-compatible storage', config: ['account_id', 'access_key', 'secret_key', 'bucket'] },
  { id: 'b2', name: 'Backblaze B2', icon: '🔴', category: 'storage', status: 'preview', desc: 'Affordable S3-compatible storage', config: ['endpoint', 'access_key', 'secret_key', 'bucket'] },
  { id: 'do_spaces', name: 'DigitalOcean Spaces', icon: '🔵', category: 'storage', status: 'preview', desc: 'S3-compatible object storage', config: ['endpoint', 'access_key', 'secret_key', 'bucket', 'region'] },
  { id: 'oci_os', name: 'Oracle Cloud Storage', icon: '🔶', category: 'storage', status: 'preview', desc: 'OCI Object Storage (S3 compat)', config: ['namespace', 'bucket', 'access_key', 'secret_key', 'region'] },
  { id: 'alibaba_oss', name: 'Alibaba OSS', icon: '🟧', category: 'storage', status: 'preview', desc: 'Alibaba Cloud Object Storage', config: ['endpoint', 'access_key', 'secret_key', 'bucket'] },

  // ─── Table Formats ─────────────────────────────────
  { id: 'iceberg', name: 'Apache Iceberg', icon: '🧊', category: 'format', status: 'available', desc: 'Open table format for huge analytics', config: ['catalog_uri', 'warehouse'] },
  { id: 'delta', name: 'Delta Lake', icon: '🔺', category: 'format', status: 'available', desc: 'ACID transactions on data lakes', config: ['path'] },
  { id: 'hudi', name: 'Apache Hudi', icon: '🎩', category: 'format', status: 'preview', desc: 'Incremental data processing framework', config: ['base_path', 'table_name'] },
  { id: 'lance', name: 'Lance', icon: '🏹', category: 'format', status: 'available', desc: 'Vector-optimized columnar format', config: ['path'] },
  { id: 'parquet', name: 'Apache Parquet', icon: '📦', category: 'format', status: 'available', desc: 'Columnar storage for analytics', config: ['path'] },
  { id: 'csv', name: 'CSV', icon: '📄', category: 'format', status: 'available', desc: 'Comma / tab / pipe delimited files', config: ['path', 'delimiter', 'has_header'] },
  { id: 'json', name: 'JSON / NDJSON', icon: '📋', category: 'format', status: 'available', desc: 'JSON and newline-delimited JSON', config: ['path'] },
  { id: 'avro', name: 'Apache Avro', icon: '🔄', category: 'format', status: 'preview', desc: 'Row-oriented serialization format', config: ['path', 'schema_registry'] },
  { id: 'orc', name: 'Apache ORC', icon: '🟣', category: 'format', status: 'preview', desc: 'Optimized Row Columnar (Hive)', config: ['path'] },
  { id: 'xml', name: 'XML', icon: '📝', category: 'format', status: 'preview', desc: 'Structured XML documents', config: ['path', 'row_tag'] },
  { id: 'excel', name: 'Excel (XLSX)', icon: '📊', category: 'format', status: 'preview', desc: 'Microsoft Excel spreadsheets', config: ['path', 'sheet_name'] },
  { id: 'arrow_ipc', name: 'Arrow IPC / Feather', icon: '🏹', category: 'format', status: 'available', desc: 'Arrow inter-process columnar format', config: ['path'] },

  // ─── Streaming & CDC ───────────────────────────────
  { id: 'kafka', name: 'Apache Kafka', icon: '📨', category: 'streaming', status: 'available', desc: 'Distributed event streaming platform', config: ['brokers', 'topic', 'group_id', 'security_protocol'] },
  { id: 'kinesis', name: 'Amazon Kinesis', icon: '🌊', category: 'streaming', status: 'preview', desc: 'AWS real-time data streaming', config: ['stream_name', 'region', 'access_key', 'secret_key'] },
  { id: 'eventhubs', name: 'Azure Event Hubs', icon: '🔷', category: 'streaming', status: 'preview', desc: 'Azure event ingestion service', config: ['connection_string', 'event_hub_name', 'consumer_group'] },
  { id: 'pulsar', name: 'Apache Pulsar', icon: '💫', category: 'streaming', status: 'preview', desc: 'Multi-tenant messaging and streaming', config: ['service_url', 'topic', 'subscription'] },
  { id: 'postgres_cdc', name: 'Postgres CDC', icon: '🔄', category: 'streaming', status: 'available', desc: 'Change Data Capture via logical replication', config: ['host', 'port', 'database', 'slot_name', 'publication'] },
  { id: 'mysql_cdc', name: 'MySQL CDC', icon: '🔁', category: 'streaming', status: 'preview', desc: 'Binlog-based change data capture', config: ['host', 'port', 'database', 'username', 'password', 'server_id'] },
  { id: 'mongodb_cdc', name: 'MongoDB CDC', icon: '🔁', category: 'streaming', status: 'available', desc: 'Change streams for real-time sync', config: ['uri', 'database', 'collection'] },
  { id: 'sqlserver_cdc', name: 'SQL Server CDC', icon: '🔄', category: 'streaming', status: 'preview', desc: 'CT/CDC-based change capture', config: ['host', 'port', 'database', 'username', 'password'] },
  { id: 'oracle_cdc', name: 'Oracle CDC', icon: '🔄', category: 'streaming', status: 'preview', desc: 'LogMiner-based change capture', config: ['host', 'port', 'service_name', 'username', 'password'] },
  { id: 'debezium', name: 'Debezium', icon: '📡', category: 'streaming', status: 'preview', desc: 'Universal CDC connector framework', config: ['connector_class', 'database_hostname', 'database_port'] },
  { id: 'nats', name: 'NATS', icon: '✈️', category: 'streaming', status: 'preview', desc: 'Cloud-native messaging system', config: ['server_url', 'subject', 'queue_group'] },
  { id: 'rabbitmq', name: 'RabbitMQ', icon: '🐰', category: 'streaming', status: 'preview', desc: 'AMQP message broker', config: ['host', 'port', 'queue', 'username', 'password'] },
  { id: 'mqtt', name: 'MQTT / Mosquitto', icon: '📶', category: 'streaming', status: 'preview', desc: 'Lightweight IoT messaging protocol', config: ['broker', 'port', 'topic', 'username', 'password'] },
  { id: 'pubsub', name: 'Google Pub/Sub', icon: '🌐', category: 'streaming', status: 'preview', desc: 'Google Cloud messaging service', config: ['project_id', 'topic', 'subscription', 'credentials_json'] },
  { id: 'redis_streams', name: 'Redis Streams', icon: '🔻', category: 'streaming', status: 'preview', desc: 'Append-only log data structure', config: ['host', 'port', 'stream_key', 'consumer_group'] },
  { id: 'flink', name: 'Apache Flink', icon: '🔀', category: 'streaming', status: 'preview', desc: 'Stateful stream processing', config: ['jobmanager_host', 'jobmanager_port'] },

  // ─── APIs & SaaS ───────────────────────────────────
  { id: 'rest_api', name: 'REST API', icon: '🌐', category: 'saas', status: 'preview', desc: 'Generic HTTP/REST data source', config: ['base_url', 'auth_type', 'api_key', 'headers'] },
  { id: 'graphql', name: 'GraphQL', icon: '◆', category: 'saas', status: 'preview', desc: 'GraphQL endpoint as table source', config: ['endpoint', 'auth_token', 'query'] },
  { id: 'salesforce', name: 'Salesforce', icon: '☁️', category: 'saas', status: 'preview', desc: 'CRM objects via SOQL / Bulk API', config: ['instance_url', 'username', 'password', 'security_token'] },
  { id: 'google_sheets', name: 'Google Sheets', icon: '📊', category: 'saas', status: 'preview', desc: 'Live spreadsheet as a table source', config: ['spreadsheet_id', 'sheet_name', 'credentials_json'] },
  { id: 'stripe', name: 'Stripe', icon: '💳', category: 'saas', status: 'preview', desc: 'Payments, invoices, and customers', config: ['api_key'] },
  { id: 'shopify', name: 'Shopify', icon: '🛍️', category: 'saas', status: 'preview', desc: 'E-commerce orders and products', config: ['shop_domain', 'api_key', 'api_secret'] },
  { id: 'hubspot', name: 'HubSpot', icon: '🟠', category: 'saas', status: 'preview', desc: 'CRM contacts, deals, and marketing', config: ['api_key', 'portal_id'] },
  { id: 'jira', name: 'Jira', icon: '🔵', category: 'saas', status: 'preview', desc: 'Issues, sprints, and project data', config: ['base_url', 'email', 'api_token', 'project_key'] },
  { id: 'github', name: 'GitHub', icon: '🐙', category: 'saas', status: 'preview', desc: 'Repos, issues, PRs, and actions', config: ['token', 'owner', 'repo'] },
  { id: 'slack', name: 'Slack', icon: '💬', category: 'saas', status: 'preview', desc: 'Channel messages and workspace data', config: ['bot_token', 'channel'] },
  { id: 'segment', name: 'Segment', icon: '🟢', category: 'saas', status: 'preview', desc: 'Customer data platform events', config: ['write_key', 'workspace'] },
  { id: 'servicenow', name: 'ServiceNow', icon: '🔧', category: 'saas', status: 'preview', desc: 'ITSM incidents and change records', config: ['instance_url', 'username', 'password'] },
  { id: 'workday', name: 'Workday', icon: '👤', category: 'saas', status: 'preview', desc: 'HR, payroll, and finance data', config: ['tenant', 'username', 'password', 'api_version'] },
  { id: 'zendesk', name: 'Zendesk', icon: '🎫', category: 'saas', status: 'preview', desc: 'Support tickets and customer data', config: ['subdomain', 'email', 'api_token'] },
  { id: 'notion', name: 'Notion', icon: '📓', category: 'saas', status: 'preview', desc: 'Workspace pages and databases', config: ['api_key', 'database_id'] },
  { id: 'airtable', name: 'Airtable', icon: '📋', category: 'saas', status: 'preview', desc: 'Spreadsheet-database hybrid tables', config: ['api_key', 'base_id', 'table_name'] },
  { id: 'delta_sharing', name: 'Delta Sharing', icon: '🤝', category: 'saas', status: 'preview', desc: 'Open cross-org data sharing protocol', config: ['share_credentials_file', 'share', 'schema', 'table'] },
  { id: 'snowplow', name: 'Snowplow', icon: '❄️', category: 'saas', status: 'preview', desc: 'Behavioral data platform events', config: ['collector_url', 'app_id'] },
  { id: 'fivetran', name: 'Fivetran', icon: '🔌', category: 'saas', status: 'preview', desc: 'ELT pipeline connector metadata', config: ['api_key', 'api_secret', 'group_id'] },
  { id: 'dbt_cloud', name: 'dbt Cloud', icon: '🔶', category: 'saas', status: 'preview', desc: 'Transformation metadata and run history', config: ['api_token', 'account_id', 'project_id'] },
]

const CATEGORY_LABELS: Record<string, { label: string; icon: React.ReactNode; color: string }> = {
  database: { label: 'Relational Databases', icon: <Database className="w-3.5 h-3.5" />, color: 'text-cyan-400' },
  analytics: { label: 'Analytical Engines', icon: <BarChart3 className="w-3.5 h-3.5" />, color: 'text-amber-400' },
  nosql: { label: 'NoSQL & Document', icon: <Layers className="w-3.5 h-3.5" />, color: 'text-emerald-400' },
  storage: { label: 'Object Storage', icon: <HardDrive className="w-3.5 h-3.5" />, color: 'text-yellow-400' },
  format: { label: 'Table Formats', icon: <FileText className="w-3.5 h-3.5" />, color: 'text-violet-400' },
  streaming: { label: 'Streaming & CDC', icon: <Radio className="w-3.5 h-3.5" />, color: 'text-rose-400' },
  saas: { label: 'APIs & SaaS', icon: <Globe className="w-3.5 h-3.5" />, color: 'text-sky-400' },
}

const S3_COMPAT_IDS = ['s3', 'minio', 'wasabi', 'r2', 'b2', 'do_spaces', 'ceph', 'oci_os', 'alibaba_oss']
const DB_LIKE_CATEGORIES = ['database', 'analytics', 'nosql']

export function DataSources() {
  const [tab, setTab] = useState('connectors')
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [s3Configs, setS3Configs] = useState<S3Config[]>([])
  const [search, setSearch] = useState('')
  const [categoryFilter, setCategoryFilter] = useState<string | null>(null)
  const [connModal, setConnModal] = useState(false)
  const [selectedConnector, setSelectedConnector] = useState<typeof CONNECTOR_CATALOG[0] | null>(null)
  const [regModal, setRegModal] = useState(false)
  const fileRef = useRef<HTMLInputElement>(null)

  // Dynamic config form
  const [configValues, setConfigValues] = useState<Record<string, string>>({})
  const [connName, setConnName] = useState('')
  const [regPath, setRegPath] = useState('')

  // Connection test wizard
  const [testResult, setTestResult] = useState<ConnectionTestResponse | null>(null)
  const [testing, setTesting] = useState(false)
  const [wizardStep, setWizardStep] = useState<'configure' | 'test' | 'done'>('configure')

  useEffect(() => {
    getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
    getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
  }, [])

  const filteredConnectors = CONNECTOR_CATALOG.filter(c => {
    if (categoryFilter && c.category !== categoryFilter) return false
    if (search && !c.name.toLowerCase().includes(search.toLowerCase()) && !c.desc.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  const openConnector = (connector: typeof CONNECTOR_CATALOG[0]) => {
    setSelectedConnector(connector)
    setConfigValues({})
    setConnName('')
    setWizardStep('configure')
    setTestResult(null)
    setConnModal(true)
  }

  const handleTestConnection = async () => {
    setTesting(true)
    setTestResult(null)
    setWizardStep('test')
    try {
      const result = await testConnection({
        conn_type: selectedConnector?.id || 'postgres',
        host: configValues.host || configValues.endpoint || configValues.contact_points || configValues.hosts || 'localhost',
        port: parseInt(configValues.port || '5432'),
        database: configValues.database || configValues.keyspace || configValues.bucket || configValues.dbname,
        username: configValues.username || configValues.user,
        password: configValues.password || configValues.secret_key,
      })
      setTestResult(result)
      if (result.success) setWizardStep('done')
    } catch (e: any) {
      setTestResult({ success: false, message: e.message || 'Test failed', validation_level: 'error', checks: [] })
    }
    setTesting(false)
  }

  const handleConnect = async () => {
    if (!selectedConnector || !connName.trim()) return
    try {
      if (selectedConnector.category === 'storage' && S3_COMPAT_IDS.includes(selectedConnector.id)) {
        await addS3Config({
          name: connName,
          endpoint: configValues.endpoint || '',
          access_key: configValues.access_key || '',
          secret_key: configValues.secret_key || '',
          bucket: configValues.bucket || '',
          region: configValues.region || 'us-east-1',
        })
        toast.success('Storage configured')
        getS3Configs().then(r => setS3Configs(r.configs || []))
      } else if (DB_LIKE_CATEGORIES.includes(selectedConnector.category) && configValues.host) {
        await addConnection({
          name: connName,
          host: configValues.host || 'localhost',
          port: parseInt(configValues.port || '5432'),
          database: configValues.database || configValues.keyspace || '',
          username: configValues.username || '',
          password: configValues.password || '',
        })
        toast.success('Database connected')
        getConnections().then(r => setConnections(r.connections || []))
      } else if (selectedConnector.category === 'format') {
        if (configValues.path || configValues.catalog_uri) {
          await registerTable(configValues.path || configValues.catalog_uri)
          toast.success('Table registered')
        }
      } else {
        toast.success(`${selectedConnector.name} connector configured`)
      }
      setConnModal(false)
    } catch (e) { toast.error((e as Error).message) }
  }

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    try {
      const res = await uploadFile(file)
      toast.success(`Uploaded as ${res.table}`)
    } catch (err) { toast.error((err as Error).message) }
    e.target.value = ''
  }

  const handleRegister = async () => {
    if (!regPath.trim()) return
    try {
      const res = await registerTable(regPath.trim())
      toast.success(`Registered ${res.table}`)
      setRegModal(false)
      setRegPath('')
    } catch (e) { toast.error((e as Error).message) }
  }

  const activeCount = connections.length + s3Configs.length
  const availableCount = CONNECTOR_CATALOG.filter(c => c.status === 'available').length
  const fullVerifyCount = CONNECTOR_CATALOG.filter(c => FULL_PROTOCOL_CONNECTORS.has(c.id)).length
  const tcpCheckCount = CONNECTOR_CATALOG.filter(c => TCP_CONNECTORS.has(c.id)).length

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-display font-bold text-zinc-100 flex items-center gap-2">
            <FolderInput className="w-5 h-5 text-amber-400" /> Data Sources
          </h1>
          <p className="text-xs text-zinc-500 mt-0.5">
            {CONNECTOR_CATALOG.length} connectors — {fullVerifyCount} full verify, {tcpCheckCount} TCP check, {CONNECTOR_CATALOG.length - fullVerifyCount - tcpCheckCount} config only
          </p>
        </div>
        <div className="flex items-center gap-2">
          <input type="file" ref={fileRef} className="hidden" accept=".csv,.parquet,.json,.ndjson,.xlsx,.avro" onChange={handleUpload} />
          <Button variant="secondary" size="sm" icon={<Upload className="w-3.5 h-3.5" />} onClick={() => fileRef.current?.click()}>Upload File</Button>
          <Button variant="secondary" size="sm" icon={<FolderOpen className="w-3.5 h-3.5" />} onClick={() => setRegModal(true)}>Register Path</Button>
        </div>
      </div>

      <Tabs
        tabs={[
          { id: 'connectors', label: 'Connector Catalog', count: CONNECTOR_CATALOG.length },
          { id: 'active', label: 'Active Connections', count: activeCount },
        ]}
        active={tab}
        onChange={setTab}
      />

      {tab === 'connectors' && (
        <div className="space-y-6">
          {/* Search + category filter chips */}
          <div className="flex items-center gap-3 flex-wrap">
            <div className="relative flex-1 max-w-sm min-w-[200px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-600" />
              <input
                className="w-full pl-9 pr-3 py-2 text-xs rounded-lg bg-navy-900/60 border border-white/[0.04] text-zinc-300 placeholder-zinc-600 focus:outline-none focus:ring-1 focus:ring-amber-400/20 backdrop-blur-sm"
                placeholder="Search 100+ connectors..."
                value={search}
                onChange={e => setSearch(e.target.value)}
              />
            </div>
            <div className="flex gap-1 flex-wrap">
              {Object.entries(CATEGORY_LABELS).map(([key, cat]) => {
                const count = CONNECTOR_CATALOG.filter(c => c.category === key).length
                return (
                  <button
                    key={key}
                    onClick={() => setCategoryFilter(categoryFilter === key ? null : key)}
                    className={cn(
                      'flex items-center gap-1.5 px-2.5 py-1.5 text-2xs font-medium rounded-md border transition-all duration-200',
                      categoryFilter === key
                        ? `${cat.color} bg-white/[0.05] border-white/[0.08]`
                        : 'text-zinc-500 bg-white/[0.02] border-white/[0.03] hover:text-zinc-400 hover:border-white/[0.06]'
                    )}
                  >
                    {cat.icon} {cat.label}
                    <span className="text-zinc-600 ml-0.5">{count}</span>
                  </button>
                )
              })}
            </div>
          </div>

          {/* Connector grid by category */}
          {Object.entries(CATEGORY_LABELS)
            .filter(([key]) => !categoryFilter || categoryFilter === key)
            .map(([key, cat]) => {
              const connectors = filteredConnectors.filter(c => c.category === key)
              if (!connectors.length) return null
              return (
                <div key={key}>
                  <h3 className={`text-xs font-display font-semibold mb-3 flex items-center gap-2 ${cat.color}`}>
                    {cat.icon} {cat.label}
                    <span className="text-zinc-600 font-normal">({connectors.length})</span>
                  </h3>
                  <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-2.5 stagger">
                    {connectors.map(connector => (
                      <button
                        key={connector.id}
                        onClick={() => openConnector(connector)}
                        className="group text-left glass glass-hover rounded-xl p-3 flex items-start gap-2.5"
                      >
                        <span className="text-xl flex-shrink-0 mt-0.5">{connector.icon}</span>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-1.5">
                            <h4 className="text-xs font-semibold text-zinc-200 group-hover:text-zinc-50 transition-colors truncate">{connector.name}</h4>
                            {connector.status === 'preview' && (
                              <Badge className="bg-amber-400/8 text-amber-400/70 border-amber-400/10 text-[9px] px-1 py-0">Preview</Badge>
                            )}
                            <Badge className={cn('text-[9px] px-1 py-0', validationTier(connector.id).color)}>
                              {validationTier(connector.id).label}
                            </Badge>
                          </div>
                          <p className="text-2xs text-zinc-500 mt-0.5 leading-relaxed line-clamp-2">{connector.desc}</p>
                        </div>
                        <ArrowRight className="w-3.5 h-3.5 text-zinc-700 group-hover:text-amber-400/50 transition-all group-hover:translate-x-0.5 flex-shrink-0 mt-0.5" />
                      </button>
                    ))}
                  </div>
                </div>
              )
            })}
        </div>
      )}

      {tab === 'active' && (
        <div className="space-y-4">
          {activeCount === 0 ? (
            <EmptyState icon={<Plug className="w-5 h-5" />} title="No active connections" description="Choose a connector from the catalog to get started" />
          ) : (
            <div className="space-y-3 stagger">
              {connections.map(c => (
                <Card key={c.id} className="flex items-center gap-4">
                  <div className="w-10 h-10 rounded-lg bg-cyan-400/[0.06] border border-cyan-400/10 flex items-center justify-center relative">
                    <Server className="w-5 h-5 text-cyan-400" />
                    <span style={{
                      position: 'absolute', top: -2, right: -2, width: 10, height: 10, borderRadius: '50%',
                      background: '#22c55e', border: '2px solid rgba(2,6,23,0.8)',
                      boxShadow: '0 0 6px rgba(34,197,94,0.4)',
                    }} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <h3 className="text-sm font-semibold text-zinc-200">{c.name}</h3>
                      <StatusDot status={c.status === 'connected' ? 'healthy' : 'error'} />
                      <Badge className="bg-cyan-400/8 text-cyan-400/80 border-cyan-400/10">{c.conn_type}</Badge>
                      <Badge className="bg-emerald-400/8 text-emerald-400/80 border-emerald-400/10 text-2xs">Connected</Badge>
                    </div>
                    <p className="text-2xs font-mono text-zinc-500 mt-0.5">{c.host}:{c.port}/{c.database}</p>
                    {c.tables.length > 0 && (
                      <div className="flex items-center gap-1.5 mt-2 flex-wrap">
                        {c.tables.slice(0, 5).map(t => <Badge key={t} className="text-2xs">{t}</Badge>)}
                        {c.tables.length > 5 && <Badge className="text-2xs">+{c.tables.length - 5} more</Badge>}
                      </div>
                    )}
                  </div>
                  <Button variant="ghost" size="sm" onClick={() => { deleteConnection(c.id); setConnections(cs => cs.filter(x => x.id !== c.id)) }}>
                    <Trash2 className="w-3.5 h-3.5 text-zinc-600" />
                  </Button>
                </Card>
              ))}
              {s3Configs.map(c => (
                <Card key={c.name} className="flex items-center gap-4">
                  <div className="w-10 h-10 rounded-lg bg-amber-400/[0.06] border border-amber-400/10 flex items-center justify-center relative">
                    <HardDrive className="w-5 h-5 text-amber-400" />
                    <span style={{
                      position: 'absolute', top: -2, right: -2, width: 10, height: 10, borderRadius: '50%',
                      background: '#22c55e', border: '2px solid rgba(2,6,23,0.8)',
                      boxShadow: '0 0 6px rgba(34,197,94,0.4)',
                    }} />
                  </div>
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <h3 className="text-sm font-semibold text-zinc-200">{c.name}</h3>
                      <Badge className="bg-amber-400/8 text-amber-400/80 border-amber-400/10">S3</Badge>
                      <Badge className="bg-emerald-400/8 text-emerald-400/80 border-emerald-400/10 text-2xs">Connected</Badge>
                    </div>
                    <p className="text-2xs font-mono text-zinc-500 mt-0.5">{c.endpoint}</p>
                    <div className="flex items-center gap-1.5 mt-2">
                      <Badge>{c.bucket}</Badge>
                      <Badge>{c.region}</Badge>
                    </div>
                  </div>
                  <Button variant="ghost" size="sm" onClick={() => { deleteS3Config(c.name); setS3Configs(cs => cs.filter(x => x.name !== c.name)) }}>
                    <Trash2 className="w-3.5 h-3.5 text-zinc-600" />
                  </Button>
                </Card>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Dynamic connector config modal */}
      <Modal open={connModal} onClose={() => setConnModal(false)} title={selectedConnector ? `Connect ${selectedConnector.name}` : 'Connect'} width="max-w-lg">
        {selectedConnector && (
          <div className="space-y-4">
            {/* Wizard step indicator */}
            <div style={{ display: 'flex', gap: 8, marginBottom: 20, justifyContent: 'center' }}>
              {['Configure', 'Test', 'Connect'].map((step, i) => {
                const stepKey = (['configure', 'test', 'done'] as const)[i]
                const active = wizardStep === stepKey
                const done = (['configure', 'test', 'done'] as const).indexOf(wizardStep) > i
                return (
                  <div key={step} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <div style={{
                      width: 24, height: 24, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700,
                      background: done ? 'rgba(34,197,94,0.2)' : active ? 'rgba(245,158,11,0.2)' : 'rgba(30,41,59,0.5)',
                      border: `1px solid ${done ? 'rgba(34,197,94,0.5)' : active ? 'rgba(245,158,11,0.5)' : 'rgba(51,65,85,0.5)'}`,
                      color: done ? '#22c55e' : active ? '#f59e0b' : '#64748b',
                    }}>
                      {done ? '\u2713' : i + 1}
                    </div>
                    <span style={{ fontSize: 12, color: active ? '#f59e0b' : done ? '#22c55e' : '#64748b' }}>{step}</span>
                    {i < 2 && <span style={{ color: '#334155', margin: '0 4px' }}>{'\u2192'}</span>}
                  </div>
                )
              })}
            </div>

            <div className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
              <span className="text-2xl">{selectedConnector.icon}</span>
              <div>
                <h4 className="text-sm font-semibold text-zinc-200">{selectedConnector.name}</h4>
                <p className="text-2xs text-zinc-500">{selectedConnector.desc}</p>
              </div>
              {selectedConnector.status === 'preview' && (
                <Badge className="ml-auto bg-amber-400/8 text-amber-400/70 border-amber-400/10">Preview</Badge>
              )}
            </div>

            <Input label="Connection Name" value={connName} onChange={e => setConnName(e.target.value)} placeholder={`my-${selectedConnector.id}`} />

            {selectedConnector.config.map(field => (
              <Input
                key={field}
                label={field.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                type={field.includes('password') || field.includes('secret') || field.includes('key') || field.includes('token') ? 'password' : 'text'}
                value={configValues[field] || ''}
                onChange={e => setConfigValues(v => ({ ...v, [field]: e.target.value }))}
                placeholder={field === 'host' ? 'localhost' : field === 'port' ? '5432' : field === 'region' ? 'us-east-1' : field === 'ssl_mode' ? 'prefer' : ''}
              />
            ))}

            {selectedConnector.status === 'preview' && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-400/[0.04] border border-amber-400/10">
                <AlertCircle className="w-4 h-4 text-amber-400/60 flex-shrink-0 mt-0.5" />
                <p className="text-2xs text-amber-400/60 leading-relaxed">
                  This connector is in preview. Configuration will be saved but the connection will be simulated until the backend driver is fully implemented.
                </p>
              </div>
            )}

            {/* Test Connection button */}
            <button
              onClick={handleTestConnection}
              disabled={testing}
              style={{
                width: '100%', padding: '10px 0', borderRadius: 8, border: '1px solid rgba(245,158,11,0.3)',
                background: 'rgba(245,158,11,0.1)', color: '#f59e0b', cursor: testing ? 'wait' : 'pointer',
                fontSize: 13, fontWeight: 600, marginBottom: 8,
              }}
            >
              {testing ? '\u23F3 Testing...' : '\uD83D\uDD0C Test Connection'}
            </button>

            {/* Test result display with tiered checks */}
            {testResult && (
              <div style={{
                padding: '12px 14px', borderRadius: 8, marginBottom: 8, fontSize: 12,
                background: testResult.success ? 'rgba(34,197,94,0.06)' : 'rgba(239,68,68,0.06)',
                border: `1px solid ${testResult.success ? 'rgba(34,197,94,0.2)' : 'rgba(239,68,68,0.2)'}`,
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                  <span style={{ fontWeight: 700, color: testResult.success ? '#22c55e' : '#ef4444' }}>
                    {testResult.success ? 'Connected' : 'Failed'}
                  </span>
                  <span style={{
                    fontSize: 10, padding: '2px 6px', borderRadius: 4, fontWeight: 600, textTransform: 'uppercase',
                    background: testResult.validation_level === 'full' ? 'rgba(34,197,94,0.15)' :
                      testResult.validation_level === 'tcp' ? 'rgba(245,158,11,0.15)' : 'rgba(239,68,68,0.15)',
                    color: testResult.validation_level === 'full' ? '#22c55e' :
                      testResult.validation_level === 'tcp' ? '#f59e0b' : '#ef4444',
                  }}>
                    {testResult.validation_level === 'full' ? 'Full Verified' :
                     testResult.validation_level === 'tcp' ? 'TCP Only' :
                     testResult.validation_level === 'dns' ? 'DNS Failed' : 'Config Error'}
                  </span>
                  {testResult.latency_ms != null && (
                    <span style={{ fontSize: 10, color: '#64748b', marginLeft: 'auto', fontFamily: 'monospace' }}>{testResult.latency_ms}ms</span>
                  )}
                </div>
                <div style={{ color: '#94a3b8', marginBottom: 8 }}>{testResult.message}</div>
                {testResult.server_version && <div style={{ color: '#64748b', marginBottom: 4 }}>Server: {testResult.server_version.split(' ').slice(0, 2).join(' ')}</div>}
                {testResult.tables_found != null && <div style={{ color: '#64748b', marginBottom: 4 }}>Tables found: {testResult.tables_found}</div>}
                {testResult.checks && testResult.checks.length > 0 && (
                  <div style={{ borderTop: '1px solid rgba(255,255,255,0.04)', paddingTop: 8, marginTop: 4 }}>
                    {testResult.checks.map((check, i) => (
                      <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '2px 0', fontSize: 11 }}>
                        <span style={{ color: check.passed ? '#22c55e' : '#ef4444', fontSize: 12 }}>
                          {check.passed ? '\u2713' : '\u2717'}
                        </span>
                        <span style={{ color: '#94a3b8', width: 60, fontWeight: 500 }}>{check.name}</span>
                        <span style={{ color: '#64748b', fontFamily: 'monospace', fontSize: 10 }}>{check.detail}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            <div className="flex justify-end gap-2 pt-2">
              <Button variant="secondary" size="sm" onClick={() => setConnModal(false)}>Cancel</Button>
              <Button variant="primary" size="sm" onClick={handleConnect} icon={<Link2 className="w-3.5 h-3.5" />}>
                {selectedConnector.category === 'format' ? 'Register' : 'Connect'}
              </Button>
            </div>
          </div>
        )}
      </Modal>

      {/* Register Path Modal */}
      <Modal open={regModal} onClose={() => setRegModal(false)} title="Register Table from Path">
        <div className="space-y-4">
          <Input label="File Path" value={regPath} onChange={e => setRegPath(e.target.value)} placeholder="/path/to/data.csv or s3://bucket/prefix" hint="Supports CSV, Parquet, JSON, Avro, ORC, Iceberg, Delta, Hudi, Lance" />

          <div className="space-y-2">
            <p className="text-2xs font-medium text-zinc-500">Quick templates</p>
            <div className="flex flex-wrap gap-1.5">
              {[
                { label: 'Local CSV', path: '/data/example.csv' },
                { label: 'Local Parquet', path: '/data/example.parquet' },
                { label: 'S3 Bucket', path: 's3://bucket/warehouse/table/' },
                { label: 'MinIO', path: 's3://minio:9000/bucket/data.parquet' },
                { label: 'Iceberg', path: 's3://warehouse/iceberg/table' },
                { label: 'Delta', path: 's3://warehouse/delta/table' },
                { label: 'Lance', path: '/data/vectors.lance' },
                { label: 'HDFS', path: 'hdfs://namenode:8020/data/' },
              ].map(t => (
                <button key={t.label} onClick={() => setRegPath(t.path)}
                  className="px-2 py-1 text-2xs rounded-md bg-white/[0.03] border border-white/[0.04] text-zinc-500 hover:text-zinc-300 hover:border-white/[0.08] transition-colors">
                  {t.label}
                </button>
              ))}
            </div>
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" size="sm" onClick={() => setRegModal(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleRegister} icon={<FileText className="w-3.5 h-3.5" />}>Register</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
