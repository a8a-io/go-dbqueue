create table queue {
    id BIGINT SEQUENCE PRIMARY KEY,
    key BINARY,
    message BINARY,
    ts BIGINT
}