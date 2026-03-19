# 07-streaming
Data Engineering Zoomcamp week 7 - streaming

## Question 1 :  Redpanda version 
    docker exec -it workshop-redpanda-1 rpk version 
    V25.3.9
## Question 2: How long did it take to send the data?
My process takes about 9 minutes to send all 49,416 recrods 
    import time
    import math 
    
    t0 = time.time()
    
    for _, row in df.iterrows():
        ride = ride_from_row(row)
        producer.send(topic_name, value=ride)
        print(f"Sent: {ride}")
        time.sleep(0.01)
    
    producer.flush() #means it is done sending all records to the topic
    
    t1 = time.time()
    print(f'took {(t1 - t0):.2f} seconds')

## Question 3 : How many trips have trip_distance > 5?
     env = StreamExecutionEnvironment.get_execution_environment()
      env.enable_checkpointing(10 * 1000)  # checkpoint every 10 seconds

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    postgres_sink = create_processed_events_sink_postgres(t_env)

    t_env.execute_sql(
        f"""
        INSERT INTO {postgres_sink}
        SELECT
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount,
            TO_TIMESTAMP_LTZ(lpep_pickup_datetime, 3) as pickup_datetime
        FROM {source_table}
        """
    ).wait()

    Select * 
    from green_trips_processed_events
    where trip_distance > 5

## Question 4 : Which PULocationID had the most trips in a single 5-minute window?
      env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_5min_aggregated_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, PULocationID;
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

    select * 
    from green_trips_5min_aggregated
    order by num_trips desc

## Question 5 : How many trips were in the longest session?
      env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_revenue
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        PARTITION BY window_start, window_end, PULocationID;

        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

## Question 6: Which hour had the highest total tip amount?
      env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start,
            window_end,
            SUM(tip_amount) AS total_tip_amount
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end;

        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

    select * from green_trips_tips_aggregated order by tip_amount desc
