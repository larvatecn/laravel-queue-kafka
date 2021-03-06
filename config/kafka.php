<?php

/**
 * This is an example of queue connection configuration.
 * It will be merged into config/queue.php.
 * You need to set proper values in `.env`.
 */
return [
    /*
     * Driver name
     */
    'driver' => 'kafka',

    /*
     * 默认队列名称
     */
    'queue' => env('KAFKA_QUEUE', 'default'),

    /*
     * The group of where the consumer in resides.
     */
    'consumer_group_id' => env('KAFKA_CONSUMER_GROUP_ID', 'laravel_queue'),

    /*
     * Address of the Kafka broker
     */
    'brokers' => env('KAFKA_BROKERS', 'localhost'),

    /*
     * 如果与kafka通信时出错，要休眠的秒数,如果设置为false，它将抛出异常，而不是在X秒内进行休眠。
     */
    'sleep_on_error' => env('KAFKA_ERROR_SLEEP', 5),

    /*
     * 检测到死锁时休眠
     */
    'sleep_on_deadlock' => env('KAFKA_DEADLOCK_SLEEP', 2),

    /*
     * 全局默认配置
     */
    'defaultConf' => [
        'enable.auto.commit' => 'false',
        'offset.store.method' => 'broker',
        //'security.protocol' => 'SASL_SSL',
        //'sasl.mechanisms' => 'PLAIN',
        //'sasl.username' => '',
        //'sasl.password' => '',
        //'ssl.ca.location' => '/ca-cert.pem',
    ]
];