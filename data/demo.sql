SELECT sk, sv, dc FROM kafka_flink_table, LATERAL TABLE (flink_dim_table(sk))
