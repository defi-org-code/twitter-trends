import {createServer, IncomingMessage, ServerResponse} from 'http';
import Twitter from './twitter';
import mongodb, {MongoClient, Db} from 'mongodb';

const fs = require('fs');

let db: Db;

const port = 5000;

const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {

    if (req.url === '/index.html') {
        res.writeHead(200, {
            'Content-Type': 'text/html'
        });
        fs.readFile('../index.html', null, function (error: any, data: Buffer) {
            if (error) {
                res.writeHead(404);
                res.write('Whoops! File not found!');
            } else {
                res.write(data);
            }
            res.end();
        });
        return;
    } else if (req.url === '/get-trends') {

        res.setHeader('Content-Type', 'application/json');

        res.end(JSON.stringify(await fetchTopEntities()));
        return;

    }

    res.end('Hello world!');
});

type Tweet = {
    id: string,
    counterToUpdate?: number,
    entities: {
        mentions: Array<{
            username: string
        }>,
        hashtags: Array<{
            tag: string
        }>,
        urls: Array<{
            url: string,
            expanded_url: string,
            display_url: string
        }>,
        cashtags: Array<{
            tag: string
        }>

    },
    public_metrics: {
        retweet_count: number,
        quote_count: number
    }
};

type Entity = {
    type: EntityType,
    name: string,
    count: number,
    lastUpdateTime: Date
};

type EntitiesResult = {
    hashtags: Array<Entity>,
    cashtags: Array<Entity>,
    mentions: Array<Entity>,
    urls: Array<Entity>
}

enum EntityType {
    CASHHASH,
    HASHTAG,
    URL,
    MENTION
}

server.listen(port, async () => {
    console.log(`Server listening on port ${port}`);

    const MongoClient: MongoClient = await mongodb.connect('mongodb://localhost:27017/twitter', {useUnifiedTopology: true});

    db = MongoClient.db('twitter');

    await processTweets();

    setInterval(async () => {
        await processTweets();
    }, 30000);
});

const processTweets = async () => {
    console.log("process tweets", new Date());

    const response = await Twitter();

    const tweets = response.includes.tweets.map((t: any): Tweet => {
        return {id: t.id, entities: t.entities, public_metrics: t.public_metrics};
    });

    for (const t of tweets) {
        t.counterToUpdate = await updateTweet(t);
    }

    await setProcessedEntities();
    await updateEntities(tweets);
};

const updateTweet = async (tweet: Tweet) => {

    let counter = tweet.public_metrics.retweet_count + tweet.public_metrics.quote_count;

    const oldTweetObject = await db.collection('tweets').findOneAndUpdate(
        {
            id: tweet.id
        },
        {
            $set: {
                counter: counter
            }
        },
        {
            upsert: true
        });

    if (!oldTweetObject.value) {
        return counter;
    }

    return counter - oldTweetObject.value.counter;
};

const setProcessedEntities = async () => {

    await db.collection('entities').updateMany(
        {"$expr": {"$ne": ["$processed", "$count"]}},
        [{
            $set: {
                processed: "$count",
                lastUpdateTime: new Date()
            }
        }]
    );

};

const updateEntities = async (tweets: Array<Tweet>) => {

    const time = new Date();

    const entities: Array<Entity> = [];

    tweets.forEach((t) => {

        if (t.entities.cashtags) {

            t.entities.cashtags.forEach(cashtag => {

                const entity = entities.find(e => e.name === cashtag.tag && e.type === EntityType.CASHHASH);

                if (entity) {
                    entity.count += t.counterToUpdate;
                } else {
                    entities.push({
                        type: EntityType.CASHHASH,
                        name: cashtag.tag,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    })
                }

            });

        }

        if (t.entities.hashtags) {

            t.entities.hashtags.forEach(hashtag => {

                const entity = entities.find(e => e.name === hashtag.tag && e.type === EntityType.HASHTAG);

                if (entity) {
                    entity.count += t.counterToUpdate;
                } else {
                    entities.push({
                        type: EntityType.HASHTAG,
                        name: hashtag.tag,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    })
                }

            });

        }

        if (t.entities.mentions) {

            t.entities.mentions.forEach(mention => {

                const entity = entities.find(e => e.name === mention.username && e.type === EntityType.MENTION);

                if (entity) {
                    entity.count += t.counterToUpdate;
                } else {
                    entities.push({
                        type: EntityType.MENTION,
                        name: mention.username,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    })
                }

            });

        }

        if (t.entities.urls) {

            t.entities.urls.forEach(url => {

                const entity = entities.find(e => e.name === url.url && e.type === EntityType.URL);

                if (entity) {
                    entity.count += t.counterToUpdate;
                } else {
                    entities.push({
                        type: EntityType.URL,
                        name: url.url,
                        count: t.counterToUpdate,
                        lastUpdateTime: time
                    })
                }

            });

        }

    });

    const bulk = db.collection('entities').initializeUnorderedBulkOp();

    entities.forEach(e => {

        bulk
            .find(
                {
                    type: e.type,
                    name: e.name
                }
            )
            .upsert()
            .updateOne(
                {
                    $set: {
                        type: e.type,
                        name: e.name,
                    },
                    $inc: {
                        count: e.count
                    }
                }
            );

    });

    return await bulk.execute();

};

const fetchTopEntities = async (): Promise<EntitiesResult> => {

    console.log('fetchTopEntities', new Date());

    const result = await db.collection('entities').aggregate(
        [
            {
                $facet: {
                    hashtags: [
                        {
                            $match: {type: EntityType.HASHTAG}
                        },
                        {
                            $sort: {processed: -1, count: -1} // desc
                        },
                        {
                            $limit: 100
                        },
                        {
                            $project: {_id: 0, type: 0}
                        }
                    ],
                    cashtags: [
                        {
                            $match: {type: EntityType.CASHHASH}
                        },
                        {
                            $sort: {processed: -1, count: -1} // desc
                        },
                        {
                            $limit: 100
                        },
                        {
                            $project: {_id: 0, type: 0}
                        }
                    ],
                    mentions: [
                        {
                            $match: {type: EntityType.MENTION}
                        },
                        {
                            $sort: {processed: -1, count: -1} // desc
                        },
                        {
                            $limit: 100
                        },
                        {
                            $project: {_id: 0, type: 0}
                        }
                    ],
                    urls: [
                        {
                            $match: {type: EntityType.URL}
                        },
                        {
                            $sort: {processed: -1, count: -1} // desc
                        },
                        {
                            $limit: 100
                        },
                        {
                            $project: {_id: 0, type: 0}
                        }
                    ]
                }
            }
        ]
    );

    const results = await result.toArray();

    return results[0];

};

const processEntities = async () => {

    const ne = {"$expr": {"$ne": ["$processed", "$count"]}};
    const func = {
        "$function": {
            "body": "function(processed, count) {return processed + Math.ceil((count - processed) / 30);}",
            "args": [
                "$processed",
                "$count"
            ],
            "lang": "js"
        }
    };

    const result = await db.collection('entities').updateMany(ne, [{"$set": {processed: func}}]);


    // const func = {
    //     "$function": {
    //         "body": function (processed: number, count: number) {
    //             return processed + Math.ceil((count - processed) / 30)
    //         },
    //         "args": [
    //             "$processed",
    //             "$count"
    //         ],
    //         "lang": "js"
    //     }
    // };
    //
    // const result = await db.collection('entities').aggregate([
    //     {
    //         "$match": {"$expr": {"$ne": ["$processed", "$count"]}},
    //         "$addFields": {processed: func}
    //     }
    // ]);

    // ne, {$set: {processed: func}}

    return result;

};