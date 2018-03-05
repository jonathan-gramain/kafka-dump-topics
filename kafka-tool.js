'use strict';

const async = require('async');
const fs = require('fs');
const program = require('commander');
const kafka = require('kafka-node');

const topics = [];

function collectTopics(topic) {
    topics.push(topic);
}

function dumpTopics() {
    const dumperVars = topics.map(topic => ({
        topic,
        outFile: `./${topic}.log`,
        nbConsumedMessages: 0,
    }));

    console.log('initializing consumers...');

    async.each(dumperVars, (vars, done) => {
        vars.stream = fs.createWriteStream(vars.outFile, { flags: 'a' });
        vars.consumer = new kafka.ConsumerGroup({
            host: program.zookeeper,
            groupId: 'debugging-consumer',
            fromOffset: 'latest',
            autoCommit: true,
            fetchMaxBytes: 100000,
        }, vars.topic);
        vars.consumer.on('error', err => {
            console.error(`error in consumer of ${vars.topic} topic: ${err.message}`);
        });
        vars.consumer.once('connect', () => {
            console.log(`    ${vars.topic} => ${vars.outFile}`);
            vars.consumer.on('message', entry => {
                vars.stream.write(`${Date.now()},${entry.value}\n`, 'utf8');
                ++vars.nbConsumedMessages;
            });
            done();
        });
    }, () => {
        process.on('SIGINT', () => {
            console.log('closing consumers and log files...');
            async.each(dumperVars, (vars, done) => {
                vars.consumer.close(true, () => {
                    console.log(`    ${vars.outFile}: ${vars.nbConsumedMessages} messages dumped`);
                    vars.stream.on('finish', done);
                    vars.stream.end();
                });
            }, () => {
                process.exit(0);
            });
        });
    });
}

function getLatestOffset() {
    const dumperVars = topics.map(topic => ({
        topic,
        nbConsumedMessages: 0,
    }));

    let client;
    let offset;
    let topicsMd;
    async.waterfall([
        next => {
            client = new kafka.Client(program.zookeeper);
            client.on('connect', next);
        },
        next => client.loadMetadataForTopics(topics, (err, res) => {
            if (err) {
                console.error('error loading metadata for topics:', err);
                return next(err);
            }
            return next(null, res[1].metadata);
        }),
        (metadata, next) => {
            topicsMd = metadata;
            offset = new kafka.Offset(client);
            offset.fetchLatestOffsets(topics, (err, offsetRes) => {
                if (err) {
                    console.error('error fetching topic offsets:', err);
                    return next(err);
                }
                return next(null, offsetRes);
            });
        },
        (offsetRes, next) => {
            topics.forEach(topic => {
                const topicOffsets = offsetRes[topic];
                console.log(`latest offsets for topic ${topic}:`);
                Object.keys(topicOffsets).forEach(partNum => {
                    const partOffset = topicOffsets[partNum];
                    console.log(`    partition ${partNum}: offset=${partOffset}`);
                });
            });
            client.close();
            return next();
        },
    ], err => {
        process.exit(err ? 1 : 0);
    });
}

function getConsumerGroupOffset(options) {
    if (!options.group) {
        console.log('you need to specify a consumer group with --group');
        process.exit(1);
    }
    async.each(topics, (topic, done) => {
        let consumer;
        async.series([
            next => {
                consumer = new kafka.ConsumerGroup({
                    host: program.zookeeper,
                    groupId: options.group,
                    fromOffset: 'none',
                    autoCommit: false,
                    fetchMaxBytes: 100000,
                }, topic);
                consumer.on('error', err => {
                    console.error(`error in consumer of ${topic} topic: ` +
                                  `${err.message}`);
                    return next(err);
                });
                return consumer.once('connect', next);
            },
            next => {
                consumer.topicPayloads.forEach(payload => {
                    console.log(`    topic ${payload.topic}, ` +
                                `partition ${payload.partition}: ` +
                                `offset=${payload.offset}`);
                });
                consumer.close(false, next);
            },
        ], done);
    }, () => {});
}

function setConsumerGroupOffset(options) {
    if (!options.group) {
        console.log('you need to specify a consumer group with --group');
        process.exit(1);
    }
    async.each(topics, (topic, done) => {
        let consumer;
        async.series([
            next => {
                consumer = new kafka.ConsumerGroup({
                    host: program.zookeeper,
                    groupId: options.group,
                    fromOffset: 'earliest',
                    autoCommit: false,
                    fetchMaxBytes: 100000,
                }, topic);
                consumer.on('error', err => {
                    console.error(`error in consumer of ${topic} topic: ` +
                                  `${err.message}`);
                    return next(err);
                });
                return consumer.once('connect', next);
            },
            next => consumer.close(true, next),
            next => {
                console.log('offset set to \'latest\' for consumer group ' +
                            `${options.group} and topic ${topic}`);
                return process.nextTick(next);
            }
        ], done);
    }, () => {});
}

program
    .option('--zookeeper <connectString>',
            'zookeeper connect string (e.g. "localhost:2181")')
    .option('--topic [topic]', 'consume topic', collectTopics, []);

program
    .command('dump-topics')
    .description('dump kafka topic messages into separate local log files')
    .action(dumpTopics);

program
    .command('get-latest-offset')
    .description('get latest offset of topic partitions')
    .action(getLatestOffset);

program
    .command('get-consumer-group-offset')
    .option('--group <groupId>', 'set group Id')
    .action(getConsumerGroupOffset);

program
    .command('set-consumer-group-offset')
    .option('--group <groupId>', 'set group Id')
    .action(setConsumerGroupOffset);


program.parse(process.argv);
