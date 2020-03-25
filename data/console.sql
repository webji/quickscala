-- SELECT sk, sv , dc FROM kafka_flink_table, LATERAL TABLE(flink_dim_table(sk))
show databases;

CREATE DATABASE flink;

use flink;

show tables;

CREATE TABLE IF NOT EXISTS `flink_kafka_table`
(
    `id`       INT UNSIGNED AUTO_INCREMENT,
    `kk`    VARCHAR(100) NOT NULL,
    `kv`   VARCHAR(40)  NOT NULL,
    `kc`   VARCHAR(40) NOT NULL ,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS flink_kafka_table;
DESCRIBE flink_kafka_table;

SELECT * FROM flink_kafka_table;

SELECT `dk`, `dc` FROM `flink_dim_table` WHERE `dk`='*'

CREATE TABLE IF NOT EXISTS `flink_dim_table`
(
    `id`       INT UNSIGNED AUTO_INCREMENT,
    `dk`    VARCHAR(100) NOT NULL,
    `dc`   VARCHAR(40)  NOT NULL,
    `update_date` DATE,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS flink_dim_table  ;

INSERT INTO flink_dim_table(dk, dc) values ('a', 'A');

INSERT INTO flink_dim_table(dk, dc) values ('b', 'B');

INSERT INTO flink_dim_table(dk, dc) values ('c', 'C');

SELECT * FROM flink_dim_table;
DESCRIBE flink_dim_table;