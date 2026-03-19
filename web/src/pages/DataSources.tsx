import { useState, useEffect, useRef, useCallback } from 'react'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Input } from '../components/ui/Input'
import { Textarea, Select } from '../components/ui/Input'
import { Drawer } from '../components/ui/Drawer'
import { Tabs } from '../components/ui/Tabs'
import { EmptyState } from '../components/ui/EmptyState'
import { StatusDot } from '../components/ui/StatusDot'
import { useNavigate } from 'react-router-dom'
import { cn } from '../lib/utils'
import { getConnections, addConnection, updateConnection, deleteConnection, getS3Configs, addS3Config, updateS3Config, deleteS3Config, uploadFile, registerTable, testConnection, importConnections, exportConnections, browseS3 } from '../api/client'
import { useServerEvents } from '../components/layout/Shell'
import type { TrinoScanEvent } from '../hooks/useEventStream'
import { useAppStore } from '../stores/app'
import type { ConnectionEntry, S3Config, ConnectionTestResponse } from '../types'
import {
  FolderInput, Database, HardDrive, Upload, Plus, Trash2, Pencil,
  Server, Globe, FileText, Plug, Link2, FolderOpen, Search,
  ArrowRight, CheckCircle2, AlertCircle, Zap, ExternalLink,
  BarChart3, Layers, Radio, Cloud, Code, Copy, Download,
} from 'lucide-react'
import toast from 'react-hot-toast'

// Validation tier per connector — what level of testing is available
const FULL_PROTOCOL_CONNECTORS = new Set([
  'postgres', 'cockroachdb', 'yugabytedb', 'timescaledb', 'greenplum', 'redshift', 'cdc_postgres',
  'mysql', 'mariadb',
])
const TCP_CONNECTORS = new Set([
  'tidb', 'vitess', 'singlestore',
  'mongodb', 'cdc_mongodb', 'cassandra', 'scylladb', 'redis',
  'elasticsearch', 'opensearch', 'neo4j', 'influxdb', 'questdb',
  'clickhouse', 'druid', 'pinot', 'starrocks', 'doris', 'trino', 'presto',
  'oracle', 'sqlserver', 'mssql', 'db2', 'sap_hana', 'teradata', 'vertica',
  'exasol', 'netezza', 'informix', 'kafka', 'minio', 'hbase',
  'sqlite',
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
  { id: 'trino', name: 'Trino', icon: '🔺', category: 'analytics', status: 'available', desc: 'Distributed SQL query engine', config: ['host', 'port', 'catalog', 'schema', 'username', 'password'] },
  { id: 'presto', name: 'PrestoDB', icon: '🎯', category: 'analytics', status: 'available', desc: 'Distributed SQL engine (Meta fork)', config: ['host', 'port', 'catalog', 'schema', 'username', 'password'] },
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
  const navigate = useNavigate()
  const [connName, setConnName] = useState('')
  const [regPath, setRegPath] = useState('')

  // MongoDB auth method
  const [mongoAuthMethod, setMongoAuthMethod] = useState<'scram' | 'aws_iam' | 'connection_string'>('scram')

  // Edit connection
  const [editingConnection, setEditingConnection] = useState<ConnectionEntry | null>(null)
  // Edit S3 connection
  const [editingS3, setEditingS3] = useState<S3Config | null>(null)

  // Connection test wizard
  const [testResult, setTestResult] = useState<ConnectionTestResponse | null>(null)
  const [testing, setTesting] = useState(false)
  const [wizardStep, setWizardStep] = useState<'configure' | 'test' | 'done'>('configure')

  // JSON Import/Export
  const [importJson, setImportJson] = useState('')
  const [importError, setImportError] = useState<string | null>(null)
  const [importResult, setImportResult] = useState<{ imported: { connections: any[]; s3_configs: any[] }; total: number; errors: string[] } | null>(null)
  const [importing, setImporting] = useState(false)

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
    setMongoAuthMethod('scram')
    setWizardStep('configure')
    setTestResult(null)
    setEditingConnection(null)
    setConnModal(true)
  }

  const handleEditConnection = (c: ConnectionEntry) => {
    const connector = CONNECTOR_CATALOG.find(cn => cn.id === c.conn_type)
    if (!connector) return
    setEditingConnection(c)
    setSelectedConnector(connector)
    setConnName(c.name)
    setConfigValues({
      host: c.host || '',
      port: String(c.port || ''),
      database: c.database || '',
      username: c.username || '',
    })
    if (c.auth_method) setMongoAuthMethod(c.auth_method as typeof mongoAuthMethod)
    setWizardStep('configure')
    setTestResult(null)
    setConnModal(true)
  }

  const handleEditS3 = (c: S3Config) => {
    const connector = CONNECTOR_CATALOG.find(cn => S3_COMPAT_IDS.includes(cn.id))
    if (!connector) return
    setEditingS3(c)
    setEditingConnection(null)
    setSelectedConnector(connector)
    setConnName(c.name)
    setConfigValues({
      endpoint: c.endpoint || '',
      access_key: c.access_key || '',
      secret_key: '', // Don't pre-fill secret — user must re-enter
      bucket: c.bucket || '',
      region: c.region || 'us-east-1',
    })
    setWizardStep('configure')
    setTestResult(null)
    setConnModal(true)
  }

  const handleTestConnection = async () => {
    setTesting(true)
    setTestResult(null)
    setWizardStep('test')
    try {
      const isMongo = selectedConnector?.id === 'mongodb' || selectedConnector?.id === 'cdc_mongodb'
      const baseParams = {
        conn_type: selectedConnector?.id || 'postgres',
        host: configValues.host || configValues.endpoint || configValues.contact_points || configValues.hosts || 'localhost',
        port: parseInt(configValues.port || '5432'),
        database: configValues.database || configValues.catalog || configValues.keyspace || configValues.bucket || configValues.dbname,
        username: configValues.username || configValues.user,
        password: configValues.password || configValues.secret_key,
      }
      // Add MongoDB-specific auth fields
      if (isMongo) {
        if (mongoAuthMethod === 'connection_string') {
          Object.assign(baseParams, {
            auth_method: 'connection_string',
            connection_string: configValues.connection_string || '',
            host: configValues.connection_string || 'localhost',
          })
        } else if (mongoAuthMethod === 'aws_iam') {
          Object.assign(baseParams, {
            auth_method: 'aws_iam',
            aws_access_key: configValues.aws_access_key || '',
            aws_secret_key: configValues.aws_secret_key || '',
            aws_session_token: configValues.aws_session_token || '',
            aws_region: configValues.aws_region || 'us-east-1',
          })
        } else {
          Object.assign(baseParams, { auth_method: 'scram' })
        }
      }
      const result = await testConnection(baseParams)
      setTestResult(result)
      if (result.success) setWizardStep('done')
    } catch (e: any) {
      setTestResult({ success: false, message: e.message || 'Test failed', validation_level: 'error', checks: [] })
    }
    setTesting(false)
  }

  // Subscribe to SSE connection sync events instead of polling
  const { onConnectionSync, onTrinoScan, onS3Scan } = useServerEvents()
  const { darkMode } = useAppStore()

  // Track Trino scan progress per connection
  const [trinoScanState, setTrinoScanState] = useState<Record<string, { phase: string; status: string }>>({})

  // Track S3 scan progress per config
  const [s3ScanState, setS3ScanState] = useState<Record<string, {
    phase: string; detail: string; scanned: number; total: number; found: number;
    elapsed_ms: number; formats: Record<string, number>
  }>>({})

  // S3 file browser state
  const [s3BrowseOpen, setS3BrowseOpen] = useState<string | null>(null) // config name
  const [s3BrowsePrefix, setS3BrowsePrefix] = useState('')
  const [s3BrowseEntries, setS3BrowseEntries] = useState<Array<{ name: string; type: string; key: string; size: number; last_modified?: string; extension?: string }>>([])
  const [s3BrowseLoading, setS3BrowseLoading] = useState(false)

  const handleBrowseS3 = async (configName: string, prefix = '') => {
    setS3BrowseOpen(configName)
    setS3BrowsePrefix(prefix)
    setS3BrowseLoading(true)
    try {
      const result = await browseS3(configName, prefix)
      setS3BrowseEntries(result.entries || [])
    } catch {
      setS3BrowseEntries([])
    } finally {
      setS3BrowseLoading(false)
    }
  }

  useEffect(() => {
    const unsub = onConnectionSync((event) => {
      setConnections(prev => prev.map(c => {
        if (c.id !== event.id) return c
        if (event.sync_status === 'ready') {
          toast.success(`${c.name}: ${event.table_count} tables ready to query`, { id: `sync-${event.id}`, duration: 5000 })
          return { ...c, tables: event.tables, sync_status: 'ready', sync_error: undefined }
        }
        if (event.sync_status === 'error') {
          toast.error(`${c.name}: ${event.sync_error || 'Connection failed'}`, { id: `sync-${event.id}`, duration: 8000 })
          return { ...c, sync_status: 'error', sync_error: event.sync_error || 'Unknown error' }
        }
        // Still syncing — update tables if growing
        return event.tables.length > (c.tables?.length || 0)
          ? { ...c, tables: event.tables }
          : c
      }))
    })
    return unsub
  }, [onConnectionSync])

  // Subscribe to Trino scan progress events
  useEffect(() => {
    const unsub = onTrinoScan((event: TrinoScanEvent) => {
      if (event.sync_status === 'ready') {
        // Scan complete — show toast and clear after brief delay for green transition
        const phase = event.phase || 'Scan complete'
        const tableMatch = phase.match(/(\d+)\s*tables?/i)
        const tableCount = tableMatch ? tableMatch[1] : ''
        toast.success(
          tableCount ? `Trino scan complete: ${tableCount} tables` : 'Trino scan complete',
          { id: `trino-scan-${event.id}` }
        )
        setTrinoScanState(prev => ({ ...prev, [event.id]: { phase, status: 'ready' } }))
        // Clear the completed state after 3 seconds
        setTimeout(() => {
          setTrinoScanState(prev => {
            const next = { ...prev }
            delete next[event.id]
            return next
          })
        }, 3000)
      } else {
        setTrinoScanState(prev => ({
          ...prev,
          [event.id]: { phase: event.phase || 'Scanning...', status: event.sync_status },
        }))
      }
    })
    return unsub
  }, [onTrinoScan])

  // Subscribe to S3 scan progress events
  useEffect(() => {
    const unsub = onS3Scan((event) => {
      if (event.sync_status !== 'syncing') {
        // Scan complete — refresh configs and clear progress
        getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
        setS3ScanState(prev => {
          const next = { ...prev }
          delete next[event.name]
          return next
        })
        if (event.found > 0) {
          const fmts = Object.entries(event.formats || {}).map(([k, v]) => `${v} ${k}`).join(', ')
          toast.success(`S3 scan complete: ${event.found} tables (${fmts})`, { id: `s3-scan-${event.name}` })
        }
      } else {
        setS3ScanState(prev => ({
          ...prev,
          [event.name]: {
            phase: event.phase || 'scanning',
            detail: event.detail || '',
            scanned: event.scanned,
            total: event.total,
            found: event.found,
            elapsed_ms: event.elapsed_ms,
            formats: event.formats || {},
          },
        }))
      }
    })
    return unsub
  }, [onS3Scan])

  const handleConnect = async () => {
    if (!selectedConnector || !connName.trim()) return
    try {
      if (selectedConnector.category === 'storage' && S3_COMPAT_IDS.includes(selectedConnector.id)) {
        const s3Payload = {
          name: connName,
          endpoint: configValues.endpoint || '',
          access_key: configValues.access_key || '',
          secret_key: configValues.secret_key || '',
          bucket: configValues.bucket || '',
          region: configValues.region || 'us-east-1',
        }
        if (editingS3) {
          await updateS3Config(editingS3.name, s3Payload)
          toast.success('S3 connection updated — re-scanning tables')
          setEditingS3(null)
        } else {
          await addS3Config(s3Payload)
          toast.success('Storage configured')
        }
        getS3Configs().then(r => setS3Configs(r.configs || []))
      } else if (DB_LIKE_CATEGORIES.includes(selectedConnector.category) && (configValues.host || (selectedConnector.id === 'mongodb' && mongoAuthMethod === 'connection_string'))) {
        const isMongo = selectedConnector.id === 'mongodb' || selectedConnector.id === 'cdc_mongodb'
        const connPayload: Parameters<typeof addConnection>[0] = {
          name: connName,
          conn_type: selectedConnector.id,
          host: configValues.host || 'localhost',
          port: parseInt(configValues.port || (isMongo ? '27017' : '5432')),
          database: configValues.database || configValues.catalog || configValues.keyspace || '',
          username: configValues.username || '',
          password: configValues.password || '',
        }
        if (isMongo) {
          connPayload.auth_method = mongoAuthMethod
          if (mongoAuthMethod === 'connection_string') {
            connPayload.connection_string = configValues.connection_string || ''
            connPayload.host = configValues.connection_string || 'localhost'
          } else if (mongoAuthMethod === 'aws_iam') {
            connPayload.aws_access_key = configValues.aws_access_key || ''
            connPayload.aws_secret_key = configValues.aws_secret_key || ''
            connPayload.aws_session_token = configValues.aws_session_token || ''
            connPayload.aws_region = configValues.aws_region || 'us-east-1'
          }
        }
        if (editingConnection) {
          const result = await updateConnection(editingConnection.id, connPayload)
          // Update in-place
          setConnections(prev => prev.map(conn => conn.id === editingConnection.id ? {
            ...conn,
            name: connName,
            host: configValues.host || conn.host,
            port: parseInt(configValues.port || String(conn.port)),
            database: configValues.database || configValues.catalog || configValues.keyspace || conn.database,
            username: configValues.username || conn.username,
            sync_status: result.sync_status === 'syncing' ? 'syncing' : 'ready',
            tables: result.tables || conn.tables,
          } : conn))
          toast.success('Connection updated')
          // Refresh connections list
          getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
          setEditingConnection(null)
        } else {
          const result = await addConnection(connPayload)
          // Add connection immediately with syncing status
          const newConn: ConnectionEntry = {
            id: result.id,
            name: connName,
            conn_type: selectedConnector.id,
            host: configValues.host || (isMongo && mongoAuthMethod === 'connection_string' ? configValues.connection_string || '' : 'localhost'),
            port: parseInt(configValues.port || (isMongo ? '27017' : '5432')),
            database: configValues.database || configValues.catalog || configValues.keyspace || '',
            username: configValues.username || '',
            status: 'connected',
            tables: [],
            created_at: new Date().toISOString(),
            mode: ['postgres', 'mysql', 'sqlite', 'clickhouse'].includes(selectedConnector.id) ? 'federated' : 'snapshot',
            sync_status: result.sync_status === 'syncing' ? 'syncing' : 'ready',
            ...(isMongo ? { auth_method: mongoAuthMethod } : {}),
          }
          setConnections(prev => [...prev, newConn])
          setTab('active')
          toast.loading('Discovering tables in background...', { id: `sync-${result.id}`, duration: 30000 })
        }
        // SSE will push sync status updates automatically
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
          { id: 'import', label: 'JSON Import' },
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
              {connections.map(c => {
                const isSyncing = c.sync_status === 'syncing'
                const isSyncError = c.sync_status === 'error'
                const isCached = c.sync_status === 'cached'
                return (
                <Card key={c.id} className={cn("flex items-center gap-4", isSyncing && "border-amber-400/20")}>
                  <div className="w-10 h-10 rounded-lg bg-cyan-400/[0.06] border border-cyan-400/10 flex items-center justify-center relative">
                    <Server className="w-5 h-5 text-cyan-400" />
                    <span style={{
                      position: 'absolute', top: -2, right: -2, width: 10, height: 10, borderRadius: '50%',
                      background: isSyncing ? '#f59e0b' : isSyncError ? '#ef4444' : isCached ? '#64748b' : '#22c55e',
                      border: '2px solid rgba(2,6,23,0.8)',
                      boxShadow: `0 0 6px ${isSyncing ? 'rgba(245,158,11,0.4)' : isSyncError ? 'rgba(239,68,68,0.4)' : 'rgba(34,197,94,0.4)'}`,
                      animation: isSyncing ? 'pulse 2s infinite' : undefined,
                    }} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <h3 className="text-sm font-semibold text-zinc-200">{c.name}</h3>
                      <StatusDot status={isSyncError ? 'error' : c.status === 'connected' ? 'healthy' : 'error'} />
                      <Badge className="bg-cyan-400/8 text-cyan-400/80 border-cyan-400/10">{c.conn_type}</Badge>
                      {['postgres', 'mysql', 'sqlite', 'clickhouse'].includes(c.conn_type) ? (
                        <Badge className="bg-violet-400/8 text-violet-400/80 border-violet-400/10 text-2xs">Federated</Badge>
                      ) : (
                        <Badge className="bg-zinc-400/8 text-zinc-400/80 border-zinc-400/10 text-2xs">Snapshot</Badge>
                      )}
                      {isSyncing ? (
                        <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20 text-2xs ">
                          Discovering tables...
                        </Badge>
                      ) : isSyncError ? (
                        <Badge className="bg-red-400/10 text-red-400 border-red-400/20 text-2xs">Sync Failed</Badge>
                      ) : isCached ? (
                        <Badge className="bg-zinc-400/10 text-zinc-400 border-zinc-400/20 text-2xs">
                          Cached {c.tables.length > 0 && `(${c.tables.length} tables)`}
                        </Badge>
                      ) : (
                        <Badge className="bg-emerald-400/8 text-emerald-400/80 border-emerald-400/10 text-2xs">
                          Connected {c.tables.length > 0 && `(${c.tables.length} tables)`}
                        </Badge>
                      )}
                    </div>
                    <p className="text-2xs font-mono text-zinc-500 mt-0.5">{c.host}:{c.port}/{c.database}</p>
                    {isCached && c.status === 'cached' && (
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-2xs text-zinc-500">Restored from cache — </span>
                        <button
                          onClick={() => handleEditConnection(c)}
                          className="text-2xs text-amber-400 font-medium hover:text-amber-300 underline transition-colors"
                        >
                          Re-enter credentials to reconnect
                        </button>
                      </div>
                    )}
                    {/* Discovery progress bar */}
                    {isSyncing && (
                      <div className="mt-2">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-2xs text-amber-400/80">Discovering tables...</span>
                          {c.tables.length > 0 && (
                            <span className="text-2xs font-mono text-amber-400/60">{c.tables.length} found</span>
                          )}
                        </div>
                        <div className={cn("h-1.5 rounded-full overflow-hidden", darkMode ? "bg-white/[0.06]" : "bg-slate-200")}>
                          <div
                            className="h-full rounded-full bg-amber-400 transition-all duration-700"
                            style={{
                              width: c.tables.length > 0 ? `${Math.min(95, c.tables.length * 5)}%` : '30%',
                              animation: 'pulse 2s ease-in-out infinite',
                            }}
                          />
                        </div>
                      </div>
                    )}
                    {isSyncError && c.sync_error && (
                      <p className="text-2xs text-red-400/80 mt-1">{c.sync_error}</p>
                    )}
                    {/* Trino scan progress indicator */}
                    {c.conn_type === 'trino' && trinoScanState[c.id] && (() => {
                      const scan = trinoScanState[c.id]
                      const isComplete = scan.status === 'ready'
                      return (
                        <div className="mt-2 space-y-1">
                          <div className={cn(
                            "h-1.5 rounded-full overflow-hidden",
                            darkMode ? "bg-white/[0.06]" : "bg-slate-200"
                          )}>
                            <div className={cn(
                              "h-full rounded-full transition-all duration-700",
                              isComplete
                                ? "w-full bg-emerald-400"
                                : "bg-amber-400 animate-trino-scan"
                            )} style={!isComplete ? {
                              backgroundImage: 'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.3) 50%, transparent 100%)',
                              backgroundSize: '200% 100%',
                              animation: 'trinoScanPulse 1.5s ease-in-out infinite',
                            } : undefined} />
                          </div>
                          <p className={cn(
                            "text-2xs font-mono transition-colors duration-300",
                            isComplete
                              ? "text-emerald-400/80"
                              : darkMode ? "text-amber-400/70" : "text-amber-600/70"
                          )}>
                            {scan.phase}
                          </p>
                        </div>
                      )
                    })()}
                    {c.tables.length > 0 && (
                      <div className="flex items-center gap-1.5 mt-2 flex-wrap">
                        {c.tables.slice(0, 5).map(t => <Badge key={t} className="text-2xs">{t}</Badge>)}
                        {c.tables.length > 5 && <Badge className="text-2xs">+{c.tables.length - 5} more</Badge>}
                      </div>
                    )}
                    {/* Quick actions */}
                    {c.status === 'connected' && c.tables.length > 0 && (
                      <div className="flex items-center gap-1.5 mt-2">
                        <button
                          onClick={() => navigate('/sql', { state: { sql: `SELECT * FROM ${c.tables[0]} LIMIT 100;` } })}
                          className="flex items-center gap-1 px-2 py-1 rounded bg-amber-400/10 border border-amber-400/20 text-amber-400 text-2xs font-medium hover:bg-amber-400/15 transition-colors"
                        >
                          <Zap className="w-2.5 h-2.5" /> Query
                        </button>
                        {(c.conn_type === 'mongodb' || c.conn_type === 'postgres') && (
                          <button
                            onClick={() => navigate('/streaming', { state: { connectionId: c.id, sourceType: c.conn_type === 'mongodb' ? 'mongodb-cdc' : 'postgres-cdc' } })}
                            className="flex items-center gap-1 px-2 py-1 rounded bg-cyan-400/10 border border-cyan-400/20 text-cyan-400 text-2xs font-medium hover:bg-cyan-400/15 transition-colors"
                          >
                            <Radio className="w-2.5 h-2.5" /> CDC Pipeline
                          </button>
                        )}
                        <button
                          onClick={() => navigate('/catalog')}
                          className="flex items-center gap-1 px-2 py-1 rounded bg-white/[0.04] border border-white/[0.06] text-zinc-400 text-2xs font-medium hover:bg-white/[0.06] transition-colors"
                        >
                          <Database className="w-2.5 h-2.5" /> Catalog
                        </button>
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-1 flex-shrink-0">
                    <Button variant="ghost" size="sm" onClick={() => handleEditConnection(c)}>
                      <Pencil className="w-3.5 h-3.5 text-zinc-500" />
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => { deleteConnection(c.id); setConnections(cs => cs.filter(x => x.id !== c.id)) }}>
                      <Trash2 className="w-3.5 h-3.5 text-zinc-600" />
                    </Button>
                  </div>
                </Card>
                )
              })}
              {s3Configs.map(c => {
                const s3SyncStatus = c.sync_status || 'configured'
                const s3IsSyncing = s3SyncStatus === 'syncing'
                const s3IsError = s3SyncStatus === 'error'
                const s3IsReady = s3SyncStatus === 'ready'
                const s3IsCached = s3SyncStatus === 'cached'
                const s3NeedsCreds = s3IsCached && !c.access_key
                const dotColor = s3IsError ? '#ef4444' : s3IsSyncing ? '#f59e0b' : s3NeedsCreds ? '#f59e0b' : '#22c55e'
                const scanProg = s3ScanState[c.name]
                const pct = scanProg && scanProg.total > 0 ? Math.round((scanProg.scanned / scanProg.total) * 100) : 0
                // Format badges for ready state
                const fmtCounts = c.format_counts || (scanProg?.formats) || {}
                const FORMAT_COLORS: Record<string, string> = {
                  iceberg: 'bg-sky-400/10 text-sky-400 border-sky-400/20',
                  delta: 'bg-yellow-400/10 text-yellow-400 border-yellow-400/20',
                  hudi: 'bg-orange-400/10 text-orange-400 border-orange-400/20',
                  parquet: 'bg-zinc-400/10 text-zinc-400 border-zinc-400/20',
                }
                return (
                <Card key={c.name} className="flex items-start gap-4">
                  <div className="w-10 h-10 rounded-lg bg-amber-400/[0.06] border border-amber-400/10 flex items-center justify-center relative flex-shrink-0 mt-0.5">
                    <HardDrive className="w-5 h-5 text-amber-400" />
                    <span style={{
                      position: 'absolute', top: -2, right: -2, width: 10, height: 10, borderRadius: '50%',
                      background: dotColor, border: '2px solid rgba(2,6,23,0.8)',
                      boxShadow: `0 0 6px ${dotColor}66`,
                      animation: s3IsSyncing ? 'pulse 2s infinite' : undefined,
                    }} />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <h3 className="text-sm font-semibold text-zinc-200">{c.name}</h3>
                      <Badge className="bg-amber-400/8 text-amber-400/80 border-amber-400/10">S3</Badge>
                      {s3IsSyncing ? (
                        <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20 text-2xs animate-pulse">
                          {scanProg ? `Scanning ${pct}%` : 'Scanning...'}
                        </Badge>
                      ) : s3IsError ? (
                        <Badge className="bg-red-400/10 text-red-400 border-red-400/20 text-2xs">Scan Failed</Badge>
                      ) : s3IsReady ? (
                        <Badge className="bg-emerald-400/8 text-emerald-400/80 border-emerald-400/10 text-2xs">
                          Ready {c.tables && c.tables.length > 0 && `(${c.tables.length} tables)`}
                        </Badge>
                      ) : s3NeedsCreds ? (
                        <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20 text-2xs">Credentials Required</Badge>
                      ) : s3IsCached ? (
                        <Badge className="bg-zinc-400/8 text-zinc-400/80 border-zinc-400/10 text-2xs">
                          Cached {c.tables && c.tables.length > 0 && `(${c.tables.length} tables)`}
                        </Badge>
                      ) : (
                        <Badge className="bg-emerald-400/8 text-emerald-400/80 border-emerald-400/10 text-2xs">Connected</Badge>
                      )}
                      {/* Format badges when ready */}
                      {s3IsReady && Object.entries(fmtCounts).map(([fmt, count]) => (
                        <Badge key={fmt} className={cn("text-2xs", FORMAT_COLORS[fmt] || '')}>
                          {count} {fmt}
                        </Badge>
                      ))}
                    </div>
                    <p className="text-2xs font-mono text-zinc-500 mt-0.5">s3://{c.bucket} {c.endpoint ? `(${c.endpoint})` : ''}</p>
                    {s3NeedsCreds && (
                      <div className="flex items-center gap-2 mt-2">
                        <span className="text-2xs text-amber-400/80">Credentials expired — re-enter to reconnect</span>
                        <button
                          onClick={() => handleEditS3(c)}
                          className="text-2xs text-amber-400 font-medium hover:text-amber-300 underline transition-colors"
                        >
                          Update Credentials
                        </button>
                      </div>
                    )}
                    {s3IsError && c.sync_error && (
                      <p className="text-2xs text-red-400/80 mt-1">{c.sync_error}</p>
                    )}

                    {/* Live scan progress */}
                    {s3IsSyncing && scanProg && (
                      <div className="mt-2 space-y-1.5">
                        {/* Progress bar */}
                        <div className="w-full h-1.5 rounded-full bg-white/[0.04] overflow-hidden">
                          <div
                            className="h-full rounded-full bg-amber-400/60 transition-all duration-300"
                            style={{ width: `${Math.max(pct, 2)}%` }}
                          />
                        </div>
                        <div className="flex items-center justify-between">
                          <p className="text-2xs text-amber-400/70 truncate max-w-[280px]">{scanProg.detail}</p>
                          <div className="flex items-center gap-3 text-2xs text-zinc-500 flex-shrink-0">
                            <span>{scanProg.scanned}/{scanProg.total} dirs</span>
                            <span>{scanProg.found} found</span>
                            <span>{(scanProg.elapsed_ms / 1000).toFixed(1)}s</span>
                          </div>
                        </div>
                        {/* Format breakdown during scan */}
                        {Object.keys(scanProg.formats).length > 0 && (
                          <div className="flex items-center gap-1.5 flex-wrap">
                            {Object.entries(scanProg.formats).map(([fmt, count]) => (
                              <Badge key={fmt} className={cn("text-2xs", FORMAT_COLORS[fmt] || '')}>
                                {count} {fmt}
                              </Badge>
                            ))}
                          </div>
                        )}
                      </div>
                    )}

                    {!s3IsSyncing && (
                      <div className="flex items-center gap-1.5 mt-2 flex-wrap">
                        <Badge>{c.bucket}</Badge>
                        <Badge>{c.region}</Badge>
                      </div>
                    )}
                    {c.tables && c.tables.length > 0 && (
                      <div className="flex items-center gap-1.5 mt-2 flex-wrap">
                        {c.tables.slice(0, 5).map((t: string) => {
                          const tblType = c.table_types?.[t] || ''
                          const isMV = tblType.toUpperCase().includes('MATERIALIZED')
                          const isView = !isMV && tblType.toUpperCase().includes('VIEW')
                          return (
                            <Badge key={t} className={cn("text-2xs", isMV && "bg-violet-400/10 text-violet-400 border-violet-400/20", isView && "bg-sky-400/10 text-sky-400 border-sky-400/20")}>
                              {isMV && 'MV: '}{isView && 'VIEW: '}{t.includes('.') ? t.split('.').pop() : t}
                            </Badge>
                          )
                        })}
                        {c.tables.length > 5 && <Badge className="text-2xs">+{c.tables.length - 5} more</Badge>}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-1 flex-shrink-0">
                    <Button variant="ghost" size="sm" onClick={() => handleBrowseS3(c.name)} title="Browse files">
                      <FolderOpen className="w-3.5 h-3.5 text-amber-400" />
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => handleEditS3(c)}>
                      <Pencil className="w-3.5 h-3.5 text-zinc-500" />
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => { deleteS3Config(c.name); setS3Configs(cs => cs.filter(x => x.name !== c.name)) }}>
                      <Trash2 className="w-3.5 h-3.5 text-zinc-600" />
                    </Button>
                  </div>
                  {/* S3 File Browser */}
                  {s3BrowseOpen === c.name && (
                    <div className="col-span-full mt-3 pt-3 border-t border-white/[0.04]">
                      {/* Breadcrumb */}
                      <div className="flex items-center gap-1 mb-2 text-2xs">
                        <button
                          onClick={() => handleBrowseS3(c.name, '')}
                          className="text-amber-400 hover:text-amber-300 font-mono"
                        >
                          s3://{c.bucket}
                        </button>
                        {s3BrowsePrefix && s3BrowsePrefix.split('/').filter(Boolean).map((part, i, arr) => {
                          const path = arr.slice(0, i + 1).join('/') + '/'
                          return (
                            <span key={path} className="flex items-center gap-1">
                              <span className="text-zinc-600">/</span>
                              <button
                                onClick={() => handleBrowseS3(c.name, path)}
                                className="text-amber-400/70 hover:text-amber-300 font-mono"
                              >
                                {part}
                              </button>
                            </span>
                          )
                        })}
                        <button
                          onClick={() => setS3BrowseOpen(null)}
                          className="ml-auto text-zinc-600 hover:text-zinc-400"
                        >
                          Close
                        </button>
                      </div>
                      {/* File list */}
                      {s3BrowseLoading ? (
                        <div className="text-2xs text-zinc-500 py-2">Loading...</div>
                      ) : s3BrowseEntries.length === 0 ? (
                        <div className="text-2xs text-zinc-600 py-2">Empty directory</div>
                      ) : (
                        <div className="max-h-[300px] overflow-y-auto rounded border border-white/[0.04]">
                          <table className="w-full text-2xs">
                            <thead>
                              <tr className="border-b border-white/[0.04] bg-white/[0.02]">
                                <th className="text-left px-2 py-1 text-zinc-500 font-semibold">Name</th>
                                <th className="text-right px-2 py-1 text-zinc-500 font-semibold w-24">Size</th>
                                <th className="text-right px-2 py-1 text-zinc-500 font-semibold w-36">Modified</th>
                              </tr>
                            </thead>
                            <tbody>
                              {s3BrowseEntries.map((entry, i) => (
                                <tr key={i} className="border-b border-white/[0.02] hover:bg-white/[0.01]">
                                  <td className="px-2 py-1.5">
                                    {entry.type === 'directory' ? (
                                      <button
                                        onClick={() => handleBrowseS3(c.name, entry.key)}
                                        className="flex items-center gap-1.5 text-amber-400/80 hover:text-amber-300 font-mono"
                                      >
                                        <FolderOpen className="w-3 h-3" />
                                        {entry.name}/
                                      </button>
                                    ) : (
                                      <span className="flex items-center gap-1.5 text-zinc-400 font-mono">
                                        <Layers className="w-3 h-3 text-zinc-600" />
                                        {entry.name}
                                      </span>
                                    )}
                                  </td>
                                  <td className="px-2 py-1.5 text-right text-zinc-500 font-mono">
                                    {entry.size > 0 ? (
                                      entry.size > 1048576 ? `${(entry.size / 1048576).toFixed(1)}MB` :
                                      entry.size > 1024 ? `${(entry.size / 1024).toFixed(1)}KB` :
                                      `${entry.size}B`
                                    ) : ''}
                                  </td>
                                  <td className="px-2 py-1.5 text-right text-zinc-600 font-mono">
                                    {entry.last_modified ? new Date(entry.last_modified).toLocaleDateString() + ' ' + new Date(entry.last_modified).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : ''}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                          <div className="px-2 py-1.5 text-2xs text-zinc-600 border-t border-white/[0.04]">
                            {s3BrowseEntries.filter(e => e.type === 'directory').length} folders, {s3BrowseEntries.filter(e => e.type === 'file').length} files
                            {s3BrowseEntries.filter(e => e.type === 'file').length > 0 && (
                              <span className="ml-2">
                                ({(() => {
                                  const total = s3BrowseEntries.filter(e => e.type === 'file').reduce((s, e) => s + e.size, 0)
                                  return total > 1048576 ? `${(total / 1048576).toFixed(1)} MB` : total > 1024 ? `${(total / 1024).toFixed(1)} KB` : `${total} B`
                                })()})
                              </span>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </Card>
                )
              })}
            </div>
          )}
        </div>
      )}

      {tab === 'import' && (
        <div className="space-y-4">
          <Card className="p-4">
            <div className="space-y-1 mb-4">
              <h3 className="text-sm font-display font-semibold text-zinc-200">Bulk Import / Export</h3>
              <p className="text-2xs text-zinc-500 leading-relaxed">
                Bulk import connections and S3 storage via JSON. Paste your config below or use the sample template.
              </p>
              <p className="text-2xs text-zinc-600">
                Supports: postgres, mysql, mongodb, trino, sqlite + S3/MinIO storage configs
              </p>
            </div>

            <div className="flex items-center gap-2 mb-3">
              <Button variant="secondary" size="sm" icon={<Copy className="w-3.5 h-3.5" />} onClick={() => {
                const sample = JSON.stringify({
                  connections: [
                    { name: 'my-postgres', conn_type: 'postgres', host: 'localhost', port: 5432, database: 'mydb', username: 'user', password: 'pass' },
                    { name: 'my-mysql', conn_type: 'mysql', host: 'localhost', port: 3306, database: 'mydb', username: 'user', password: 'pass' },
                    { name: 'my-mongodb', conn_type: 'mongodb', host: 'localhost', port: 27017, database: 'mydb', username: 'user', password: 'pass', auth_method: 'scram' },
                    { name: 'my-trino', conn_type: 'trino', host: 'localhost', port: 8080, database: 'postgresql', username: 'admin', password: '' },
                    {
                      name: 'atlas-mongo-iam',
                      conn_type: 'mongodb',
                      host: 'cluster0.abc123.mongodb.net',
                      port: 27017,
                      database: 'mydb',
                      username: '',
                      password: '',
                      auth_method: 'aws_iam',
                      aws_access_key: 'AKIAIOSFODNN7EXAMPLE',
                      aws_secret_key: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                      aws_session_token: 'FwoGZXIvYXdzEBYaDHqa0AP...(your session token)',
                      aws_region: 'us-east-1'
                    },
                    {
                      name: 'atlas-connection-string',
                      conn_type: 'mongodb',
                      host: 'cluster0.abc123.mongodb.net',
                      port: 27017,
                      database: 'mydb',
                      username: '',
                      password: '',
                      auth_method: 'connection_string',
                      connection_string: 'mongodb+srv://user:pass@cluster0.abc123.mongodb.net/mydb?retryWrites=true&w=majority'
                    },
                  ],
                  s3_configs: [
                    { name: 'my-warehouse', endpoint: 'https://s3.amazonaws.com', access_key: 'AKIA...', secret_key: 'secret...', bucket: 'my-iceberg-warehouse', region: 'us-east-1' },
                    { name: 'local-minio', endpoint: 'http://localhost:9000', access_key: 'minioadmin', secret_key: 'minioadmin', bucket: 'data-lake', region: 'us-east-1' },
                  ],
                }, null, 2)
                navigator.clipboard.writeText(sample)
                toast.success('Sample JSON copied to clipboard')
              }}>
                Copy Sample
              </Button>
              <Button variant="secondary" size="sm" icon={<Download className="w-3.5 h-3.5" />} onClick={async () => {
                try {
                  const data = await exportConnections()
                  setImportJson(JSON.stringify(data, null, 2))
                  setImportError(null)
                  setImportResult(null)
                  toast.success('Current config loaded into editor')
                } catch (err: any) {
                  toast.error(err.message || 'Failed to export config')
                }
              }}>
                Export Current Config
              </Button>
              <div className="flex-1" />
              <Button variant="primary" size="sm" icon={<Code className="w-3.5 h-3.5" />} disabled={importing || !importJson.trim()} onClick={async () => {
                setImportError(null)
                setImportResult(null)
                let parsed: any
                try {
                  parsed = JSON.parse(importJson)
                } catch {
                  setImportError('Invalid JSON — please check syntax and try again')
                  return
                }
                if (!parsed.connections && !parsed.s3_configs) {
                  setImportError('JSON must contain "connections" and/or "s3_configs" arrays')
                  return
                }
                setImporting(true)
                try {
                  const result = await importConnections(parsed)
                  setImportResult(result)
                  toast.success(`Imported ${result.total} items`)
                  // Refresh connections and S3 configs
                  getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
                  getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
                } catch (err: any) {
                  setImportError(err.message || 'Import failed')
                } finally {
                  setImporting(false)
                }
              }}>
                {importing ? 'Importing...' : 'Import'}
              </Button>
            </div>

            {importError && (
              <div className="flex items-center gap-2 p-2.5 mb-3 rounded-lg bg-red-500/10 border border-red-500/20">
                <AlertCircle className="w-3.5 h-3.5 text-red-400 flex-shrink-0" />
                <span className="text-2xs text-red-400">{importError}</span>
              </div>
            )}

            {importResult && (
              <div className="p-3 mb-3 rounded-lg bg-emerald-500/10 border border-emerald-500/20 space-y-1.5">
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                  <span className="text-xs font-medium text-emerald-300">Import Complete</span>
                </div>
                <div className="flex gap-4 text-2xs text-zinc-400">
                  <span>{importResult.imported.connections?.length ?? 0} connections added</span>
                  <span>{importResult.imported.s3_configs?.length ?? 0} S3 configs added</span>
                  <span className="text-zinc-500">{importResult.total} total</span>
                </div>
                {importResult.errors.length > 0 && (
                  <div className="mt-1.5 space-y-0.5">
                    {importResult.errors.map((e, i) => (
                      <div key={i} className="flex items-center gap-1.5 text-2xs text-red-400">
                        <AlertCircle className="w-3 h-3 flex-shrink-0" />
                        {e}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            <textarea
              className="w-full rounded-lg bg-navy-900/80 border border-white/[0.06] text-xs text-cyan-300 font-mono p-3 placeholder-zinc-600 focus:outline-none focus:ring-1 focus:ring-amber-400/20 resize-y"
              style={{ minHeight: 400 }}
              placeholder={'{\n  "connections": [\n    { "name": "my-db", "conn_type": "postgres", "host": "...", "port": 5432, "database": "...", "username": "...", "password": "..." },\n    { "name": "atlas", "conn_type": "mongodb", "host": "cluster.mongodb.net", "database": "...", "auth_method": "aws_iam", "aws_access_key": "AKIA...", "aws_secret_key": "...", "aws_session_token": "..." }\n  ],\n  "s3_configs": [\n    { "name": "warehouse", "endpoint": "https://s3.amazonaws.com", "access_key": "...", "secret_key": "...", "bucket": "...", "region": "us-east-1" }\n  ]\n}'}
              value={importJson}
              onChange={e => {
                setImportJson(e.target.value)
                setImportError(null)
              }}
              spellCheck={false}
            />
          </Card>
        </div>
      )}

      {/* Dynamic connector config modal */}
      <Drawer open={connModal} onClose={() => { setConnModal(false); setEditingConnection(null); setEditingS3(null) }} title={selectedConnector ? (editingConnection || editingS3 ? `Edit ${selectedConnector.name}` : `Connect ${selectedConnector.name}`) : 'Connect'} subtitle="Configure and test your data source connection" width="max-w-lg">
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

            {/* MongoDB auth method selector */}
            {(selectedConnector.id === 'mongodb' || selectedConnector.id === 'cdc_mongodb') ? (
              <>
                <Select
                  label="Authentication Method"
                  value={mongoAuthMethod}
                  onChange={e => { setMongoAuthMethod(e.target.value as typeof mongoAuthMethod); setConfigValues({}) }}
                  options={[
                    { value: 'scram', label: 'SCRAM (Username/Password)' },
                    { value: 'aws_iam', label: 'AWS IAM' },
                    { value: 'connection_string', label: 'Connection String' },
                  ]}
                />

                {mongoAuthMethod === 'scram' && (
                  <>
                    <Input label="Host" value={configValues.host || ''} onChange={e => setConfigValues(v => ({ ...v, host: e.target.value }))} placeholder="localhost" />
                    <Input label="Port" value={configValues.port || ''} onChange={e => setConfigValues(v => ({ ...v, port: e.target.value }))} placeholder="27017" />
                    <Input label="Database" value={configValues.database || ''} onChange={e => setConfigValues(v => ({ ...v, database: e.target.value }))} placeholder="mydb" />
                    <Input label="Username" value={configValues.username || ''} onChange={e => setConfigValues(v => ({ ...v, username: e.target.value }))} placeholder="admin" />
                    <Input label="Password" type="password" value={configValues.password || ''} onChange={e => setConfigValues(v => ({ ...v, password: e.target.value }))} />
                  </>
                )}

                {mongoAuthMethod === 'aws_iam' && (
                  <>
                    <Input label="Host (Atlas Cluster URL)" value={configValues.host || ''} onChange={e => setConfigValues(v => ({ ...v, host: e.target.value }))} placeholder="cluster0.abc123.mongodb.net" />
                    <Input label="Database" value={configValues.database || ''} onChange={e => setConfigValues(v => ({ ...v, database: e.target.value }))} placeholder="mydb" />
                    <Input label="AWS Access Key" type="password" value={configValues.aws_access_key || ''} onChange={e => setConfigValues(v => ({ ...v, aws_access_key: e.target.value }))} placeholder="AKIA..." />
                    <Input label="AWS Secret Key" type="password" value={configValues.aws_secret_key || ''} onChange={e => setConfigValues(v => ({ ...v, aws_secret_key: e.target.value }))} />
                    <Input label="AWS Session Token (optional)" type="password" value={configValues.aws_session_token || ''} onChange={e => setConfigValues(v => ({ ...v, aws_session_token: e.target.value }))} placeholder="Optional — for temporary credentials" />
                    <Input label="AWS Region" value={configValues.aws_region || ''} onChange={e => setConfigValues(v => ({ ...v, aws_region: e.target.value }))} placeholder="us-east-1" />
                  </>
                )}

                {mongoAuthMethod === 'connection_string' && (
                  <>
                    <Input label="Connection String" value={configValues.connection_string || ''} onChange={e => setConfigValues(v => ({ ...v, connection_string: e.target.value }))} placeholder="mongodb+srv://user:pass@cluster0.abc123.mongodb.net" />
                    <Input label="Database" value={configValues.database || ''} onChange={e => setConfigValues(v => ({ ...v, database: e.target.value }))} placeholder="mydb" />
                  </>
                )}
              </>
            ) : (
              selectedConnector.config.map(field => (
                <Input
                  key={field}
                  label={field.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                  type={field.includes('password') || field.includes('secret') || field.includes('key') || field.includes('token') ? 'password' : 'text'}
                  value={configValues[field] || ''}
                  onChange={e => setConfigValues(v => ({ ...v, [field]: e.target.value }))}
                  placeholder={field === 'host' ? 'localhost' : field === 'port' ? (selectedConnector.id === 'trino' || selectedConnector.id === 'presto' ? '8080' : '5432') : field === 'catalog' ? 'postgresql' : field === 'schema' ? 'public' : field === 'region' ? 'us-east-1' : field === 'ssl_mode' ? 'prefer' : ''}
                />
              ))
            )}

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

            <div className="flex justify-end gap-2 pt-4 border-t border-white/[0.06] mt-4">
              <Button variant="secondary" size="sm" onClick={() => setConnModal(false)}>Cancel</Button>
              <Button variant="primary" size="sm" onClick={handleConnect} icon={<Link2 className="w-3.5 h-3.5" />}>
                {editingConnection || editingS3 ? 'Update' : selectedConnector.category === 'format' ? 'Register' : 'Connect'}
              </Button>
            </div>
          </div>
        )}
      </Drawer>

      {/* Register Path Modal */}
      <Drawer open={regModal} onClose={() => setRegModal(false)} title="Register Table from Path" subtitle="Register a file or directory as a queryable table" width="max-w-md">
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

          <div className="flex justify-end gap-2 pt-4 border-t border-white/[0.06] mt-4">
            <Button variant="secondary" size="sm" onClick={() => setRegModal(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleRegister} icon={<FileText className="w-3.5 h-3.5" />}>Register</Button>
          </div>
        </div>
      </Drawer>
    </div>
  )
}
