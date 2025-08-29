# MySQL Flashback

The most perverted MySQL DML flashback tool

- [MySQL Flashback](#mysql-flashback)
  - [Principle](#principle)
  - [Reason for reworking this feature](#reason-for-reworking-this-feature)
  - [Build Binary](#build-binary)
  - [Supported Features](#supported-features)
    - [What everyone has, I have too](#what-everyone-has-i-have-too)
    - [My highlights](#my-highlights)
  - [Generate Rollback Statements](#generate-rollback-statements)
    - [Cool way to play](#cool-way-to-play)
    - [You can specify multiple SQL](#you-can-specify-multiple-sql)
    - [Old way to play](#old-way-to-play)
  - [Execute Rollback SQL](#execute-rollback-sql)
    - [Rollback principle and precautions](#rollback-principle-and-precautions)
    - [Executing rollback is simple](#executing-rollback-is-simple)
  - [Offline Binlog File Generate Rollback Information](#offline-binlog-file-generate-rollback-information)

## Principle

In the era when MySQL Binlog has been played to death, everyone should know the principle by now. It's nothing more than parsing the Binlog to generate rollback SQL statements in reverse.

## Reason for reworking this feature

Frankly speaking, there are many implementations of such tools online, but none are satisfactory.

Most online implementations are developed in Python. However, when dealing with large volumes, there are often various problems. So I decided to reimplement one in Golang.

> DBAs who know a bit of Python should be familiar with the `python-mysql-replication` package for parsing MySQL Binlog.
> The `python-mysql-replication` package has a timestamp bug, I don't know if it's been fixed. I had fixed it in my project before, but didn't submit a PR.

## Build Binary

```
go build
```

## Supported Features

### What everyone has, I have too

1. Specify start and end rollback positions

2. Specify start and end rollback times

3. Start time and position can be mixed, if both are specified, position takes precedence

4. Specify multiple databases and tables

5. Specify thread id

### My highlights

1. Can use specified SQL statements to get the parameters needed for rollback above

2. Can support conditional filtering, conditional filtering is also expressed in SQL form

## Generate Rollback Statements

### Cool way to play

```
./mysql-flashback create \
    --match-sql="SELECT col_1, col_2, col_3 FROM schema.table WHERE col_1 = 1 AND col_2 IN(1, 2, 3) AND col_3 BETWEEN 10 AND 20 AND start_log_file = 'mysql-bin.000001' AND start_log_pos = 4 AND end_log_file = 'mysql-bin.000004' AND end_log_pos = 0 AND start_rollback_time = '2019-06-06 12:00:01' AND end_rollback_time = '2019-06-07 12:00:01' AND thread_id = 0"
```

The `--match-sql` parameter value is quite long, mainly because the parameters have to be in one string.

Formatted look at the `--match-sql` parameter value

```
SELECT col_1, col_2, col_3                           -- Specify only the fields needed, SELECT * means all fields
FROM schema.table                                    -- Table to rollback. Must explicitly specify the database the table belongs to
WHERE col_1 = 1                                      -- Filter condition
    AND col_2 IN(1, 2, 3)                            -- Filter condition IN expression
    AND col_3 BETWEEN 10 AND 20                      -- Filter condition BETWEEN ... AND ... expression
    AND start_log_file = 'mysql-bin.000001'          -- Specify rollback range (start binlog [file]), not a filter condition.
    AND start_log_pos = 4                            -- Specify rollback range (start binlog [position]), not a filter condition.
    AND end_log_file = 'mysql-bin.000004'            -- Specify rollback range (end binlog [file]), not a filter condition.
    AND end_log_pos = 0                              -- Specify rollback range (end binlog [position]), not a filter condition.
    AND start_rollback_time = '2019-06-06 12:00:01'  -- Specify rollback range (start time), not a filter condition.
    AND end_rollback_time = '2019-06-07 12:00:01'    -- Specify rollback range (end time), not a filter condition.
    AND thread_id = 0                                -- Specify Thread ID to rollback.
```

After formatting the SQL to look nice, it's clear what the SQL is doing:

* The following conditions specify the parameters for parsing Binlog.

```
    AND start_log_file = 'mysql-bin.000001'          -- Specify rollback range (start binlog [file]), not a filter condition.
    AND start_log_pos = 4                            -- Specify rollback range (start binlog [position]), not a filter condition.
    AND end_log_file = 'mysql-bin.000004'            -- Specify rollback range (end binlog [file]), not a filter condition.
    AND end_log_pos = 0                              -- Specify rollback range (end binlog [position]), not a filter condition.
    AND start_rollback_time = '2019-06-06 12:00:01'  -- Specify rollback range (start time), not a filter condition.
    AND end_rollback_time = '2019-06-07 12:00:01'    -- Specify rollback range (end time), not a filter condition.
    AND thread_id = 0                                -- Specify Thread ID to rollback.
```

* The following conditions specify filtering on the data. When generating rollback statements, it will match the conditions you specify.

```
WHERE col_1 = 1                                      -- Filter condition
    AND col_2 IN(1, 2, 3)                            -- Filter condition IN expression
    AND col_3 BETWEEN 10 AND 20                      -- Filter condition BETWEEN ... AND ... expression
```

* The following specifies the table to rollback

```
FROM schema.table                                    -- Table to rollback. Must explicitly specify the database the table belongs to
```

* The following specifies which fields the generated rollback statement should include, mainly for UPDATE, for INSERT it must be all field values.

```
SELECT col_1, col_2, col_3                           -- Specify only the fields needed, SELECT * means all fields
```

### You can specify multiple SQL

```
./mysql-flashback create \
    --match-sql="Specified SQL1" \
    --match-sql="Specified SQL2"
```

### Old way to play

Traditional way of specifying parameter values, you can execute `./mysql-flashback create --help` to see usage examples

```
./mysql-flashback create --help
Generate rollback sql. As follows:
Example:
Specify start position and end position
./mysql-flashback create \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-log-file="mysql-bin.000092" \
    --end-log-pos=424 \
    --thread-id=15 \
    --rollback-table="schema1.table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --match-sql="select * from schema1.table1 where name = 'aa'"

Specify start position and end time
./mysql-flashback create \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-time="2018-12-17 15:36:58" \
    --thread-id=15 \
    --rollback-table="schema1.table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --match-sql="select name, age from schema1.table1 where name = 'aa'"

Specify start time and end time
./mysql-flashback create \
    --start-time="2018-12-14 15:00:00" \
    --end-time="2018-12-17 15:36:58" \
    --thread-id=15 \
    --rollback-schema="schema1" \
    --rollback-table="table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --match-sql="select name, age from schema1.table1 where name = 'aa' and age = 2"

Usage:
  mysql-flashback create [flags]

Flags:
      --db-auto-commit            Database auto commit (default true)
      --db-charset string         Database charset (default "utf8mb4")
      --db-host string            Database host (default "127.0.0.1")
      --db-max-idel-conns int     Database max idle connections (default 8)
      --db-max-open-conns int     Database max open connections (default 8)
      --db-password string        Database password (default "root")
      --db-password-is-decrypt    Whether database password needs decryption (default true)
      --db-port int               Database port (default 3306)
      --db-schema string          Database name
      --db-timeout int            Database timeout (default 10)
      --db-username string        Database username (default "root")
      --enable-rollback-delete    Whether to enable rollback delete (default true)
      --enable-rollback-insert    Whether to enable rollback insert (default true)
      --enable-rollback-update    Whether to enable rollback update (default true)
      --end-log-file string       End log file
      --end-log-pos uint32        End log file position
      --end-time string           End time
  -h, --help                      help for create
      --match-sql string          Use simple SELECT statement to match needed fields and records
      --rollback-schema strings   Specify databases to rollback, this command can specify multiple
      --rollback-table strings    Tables to rollback, this command can specify multiple
      --save-dir string           Path to save related files
      --start-log-file string     Start log file
      --start-log-pos uint32      Start log file position
      --start-time string         Start time
      --thread-id uint32          Thread id to rollback
```

## Execute Rollback SQL

After executing `./mysql-flashback create ...`, two SQL files will be generated

1. The original SQL statement file, the statements in the file are not the statements executed at that time, but statements representing the amount of data affected. For example: An UPDATE statement that affects 10 rows, then the original SQL statement will also be 10 statements.

2. The rollback SQL file

### Rollback principle and precautions

1. Since the rollback statements are written to the file in sequence, when we rollback, we need to read the SQL file in reverse order for rollback.

2. When looking at the rollback statements, you will find that each SQL has a comment `/* crc32:xxx */` in front, the value `xxx` here records a crc32 value of each data's primary key. This is mainly recorded for concurrency.

```
/* crc32:2313941001 */ INSERT INTO `employees`.`emp1`(`emp_no`, `birth_date`, `first_name`, `last_name`, `gender`, `hire_date`) VALUES(10008, "1958-02-19", "Saniya", "Kalloufi", 1, "1994-09-15");
```

### Executing rollback is simple

Use `./mysql-flashback execute --help` to see that there is a usage example

```
./mysql-flashback execute --help
Execute the specified sql rollback file in reverse order. As follows:
Example:
./mysql-flashback execute \
    --filepath="/tmp/test.sql" \
    --paraller=8 \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root"

Usage:
  mysql-flashback execute [flags]

Flags:
      --db-auto-commit           Database auto commit (default true)
      --db-charset string        Database charset (default "utf8mb4")
      --db-host string           Database host
      --db-max-idel-conns int    Database max idle connections (default 8)
      --db-max-open-conns int    Database max open connections (default 8)
      --db-password string       Database password
      --db-password-is-decrypt   Whether database password needs decryption (default true)
      --db-port int              Database port (default -1)
      --db-schema string         Database name
      --db-timeout int           Database timeout (default 10)
      --db-username string       Database username
      --filepath string          Specified file to execute
  -h, --help                     help for execute
      --paraller int             Number of rollback parallels (default 1)
```

## Offline Binlog File Generate Rollback Information

The rollback information generation shown earlier uses simulating a slave connection to MySQL to get binlog events, but often the MySQL connection no longer exists, so you need to specify offline binlog for generating rollback information

```
Parse offline binlog, generate rollback SQL. As follows:
Example:
./mysql-flashback offline \
    --enable-rollback-insert=true \
    --enable-rollback-update=true \
    --enable-rollback-delete=true \
    --thread-id=15 \
    --save-dir="" \
    --schema-file="" \
    --match-sql="select * from schema1.table1 where name = 'aa'" \
    --match-sql="select * from schema2.table1 where name = 'aa'" \
    --binlog-file="mysql-bin.0000001" \
    --binlog-file="mysql-bin.0000002"

Usage:
  mysql-flashback offline [flags]

Flags:
      --binlog-file stringArray   Which binlog files
      --enable-rollback-delete    Whether to enable rollback delete (default true)
      --enable-rollback-insert    Whether to enable rollback insert (default true)
      --enable-rollback-update    Whether to enable rollback update (default true)
  -h, --help                      help for offline
      --match-sql stringArray     Use simple SELECT statement to match needed fields and records
      --save-dir string           Path to save related files
      --schema-file string        Table structure file
      --thread-id uint32          Thread id to rollback
```

## Offline Parse Binlog File Generate Related Statistics

Generate 4 types of statistics, saved in related directory files:

1. **offline_stat_output/table_stat.txt:** Table-related statistics (sorted by affected rows)

2. **offline_stat_output/thread_stat.txt:** ThreadId-related statistics (sorted by affected rows)

3. **offline_stat_output/timestamp_stat.txt:** Time-related statistics, statistics by seconds, the time recorded is when the transaction executes BEGIN, one time point may have multiple transactions.

4. **offline_stat_output/xid_stat.txt:** Transaction-related statistics, statistics for each transaction, sorted by xid representing different transactions

### Usage Method

```
./mysql-flashback offline-stat --help
Parse offline binlog, generate binlog statistics. As follows:
After successful execution, 4 files will be generated in the current directory
offline_stat_output/table_stat.txt # Save table statistics
offline_stat_output/thread_stat.txt # Save thread statistics
offline_stat_output/timestamp_stat.txt # Save time statistics (records the time when each transaction executes BEGIN)
offline_stat_output/xid_stat.txt # Save xid statistics

Example:
./mysql-flashback offline-stat \
    --save-dir="offline_stat_output" \
    --binlog-file="mysql-bin.0000001" \
    --binlog-file="mysql-bin.0000002"

Usage:
  mysql-flashback offline-stat [flags]

Flags:
      --binlog-file stringArray   Which binlog files
  -h, --help                      help for offline-stat
      --save-dir string           Statistics save directory (default "offline_stat_output")
```

### Examples of each file

#### table_stat.txt

```
Table: employees.emp_01 	dml affected rows: 100, insert: 100, update: 0, delete: 0, table occurrences: 1
Table: employees.emp 	dml affected rows: 10, insert: 0, update: 7, delete: 3, table occurrences: 3
```

#### thread_stat.txt

```
threadId: 464	dml affected rows: 110, insert: 100, update: 7, delete: 3, table occurrences: 4
```

#### timestamp_stat.txt

```
2023-08-11 14:35:06: dml affected rows: 1, insert: 0, update: 0, delete: 1, transactions: 1, start position: /Users/hh/Desktop/mysql-bin.000200:395
2023-08-11 14:35:20: dml affected rows: 2, insert: 0, update: 0, delete: 2, transactions: 1, start position: /Users/hh/Desktop/mysql-bin.000200:749
2023-08-11 14:36:22: dml affected rows: 100, insert: 100, update: 0, delete: 0, transactions: 1, start position: /Users/hh/Desktop/mysql-bin.000200:1147
2023-08-11 14:37:28: dml affected rows: 7, insert: 0, update: 7, delete: 0, transactions: 1, start position: /Users/hh/Desktop/mysql-bin.000200:4217
```

#### xid_stat.txt

```
Xid: 7500 	2023-08-11 14:35:06 	 dml affected rows: 1, insert: 0, update: 0, delete: 1, start position: /Users/hh/Desktop/mysql-bin.000200:395
Xid: 7501 	2023-08-11 14:35:20 	 dml affected rows: 2, insert: 0, update: 0, delete: 2, start position: /Users/hh/Desktop/mysql-bin.000200:749
Xid: 7504 	2023-08-11 14:36:22 	 dml affected rows: 100, insert: 100, update: 0, delete: 0, start position: /Users/hh/Desktop/mysql-bin.000200:1147
Xid: 7507 	2023-08-11 14:37:28 	 dml affected rows: 7, insert: 0, update: 7, delete: 0, start position: /Users/hh/Desktop/mysql-bin.000200:4217
```

## Security

This project implements several security best practices to minimize vulnerabilities:

### Container Security

- **Distroless Base Image**: Uses `gcr.io/distroless/static-debian12:nonroot` for minimal attack surface
- **Non-root User**: Runs as `nonroot:nonroot` user with minimal privileges
- **No Package Managers**: Final image contains no package managers or shells
- **Static Binary**: Compiled with static linking and stripped symbols
- **Read-only Filesystem**: Container runs with read-only root filesystem

### Build Security

- **Security Updates**: Alpine base image is updated with latest security patches
- **Dependency Scanning**: Regular vulnerability checks on Go dependencies
- **Secure Build Flags**: Uses `-w -s` flags to strip debugging information
- **Multi-stage Build**: Separates build and runtime environments

### Security Scanning

Run security scans using the provided tools:

```bash
# Run comprehensive security scan
./security-scan.sh

# Build and scan Docker image
make docker-build-scan

# Scan source code for vulnerabilities
make security-scan
```

### Security Tools

Install recommended security scanning tools:

```bash
# Vulnerability scanner
brew install trivy

# Docker image scanner
brew install goodwithtech/r/dockle

# Go security linter
go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest

# Go vulnerability checker
go install golang.org/x/vuln/cmd/govulncheck@latest

# Secret scanner
brew install gitleaks
```

### Security Best Practices

1. **Regular Updates**: Keep dependencies and base images updated
2. **Secret Management**: Never hardcode credentials in source code
3. **Network Security**: Use TLS/SSL for database connections
4. **Access Control**: Limit database user privileges to minimum required
5. **Audit Logging**: Enable comprehensive logging for security monitoring
6. **Backup Security**: Encrypt and secure backup files
7. **Testing**: Test rollback procedures in staging before production use

### Docker Security Options

When running the container, use additional security options:

```bash
docker run --rm \
  --user nonroot:nonroot \
  --read-only \
  --tmpfs /tmp \
  --security-opt no-new-privileges \
  --cap-drop ALL \
  mysql-flashback:latest --help
```

### Reporting Security Issues

If you discover a security vulnerability, please report it responsibly by:
1. Creating a GitHub issue with "SECURITY" in the title
2. Providing detailed information about the vulnerability
3. Allowing time for the issue to be addressed before public disclosure
