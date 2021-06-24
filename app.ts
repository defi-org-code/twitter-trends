import {createServer, IncomingMessage, ServerResponse} from 'http';
import getRecentTweets from './twitter';
import sqlite3,{Database} from 'better-sqlite3';
import {EntitiesResult, Entity, EntityType, Tweet} from "./types";

const fs = require('fs');
let db:Database;
const port = 5000;

const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {

    if (req.url === '/index.html') {
        res.writeHead(200, {
            'Content-Type': 'text/html'
        });
        fs.readFile('./index.html', null, function (error: any, data: Buffer) {
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

        const result = fs.readFileSync('./top-entities.json', 'utf8');

        res.end(result);
        return;

    }

    res.end('Hello world!');
});

server.listen(port, async () => {
    console.log(`Server listening on port ${port}`);

    db = sqlite3('./twitter.db');

    db.exec('CREATE TABLE IF NOT EXISTS tweets (id TEXT PRIMARY KEY, count INTEGER)');
    db.exec('CREATE TABLE IF NOT EXISTS entities (name TEXT, type INTEGER, count INTEGER, processed INTEGER, lastUpdateTime TEXT, PRIMARY KEY (name, type))');

    console.log("db");

    //await truncateEntities();

    await processTweets();

    // setInterval(async () => {
    //     await processTweets();
    // }, 30000);
});

const processTweets = async () => {
    console.log("process tweets", new Date());

    const response = await getRecentTweets();

    const tweets = response.includes.tweets.map((t: any): Tweet => {
        return {id: t.id, entities: t.entities, public_metrics: t.public_metrics};
    });

    for (const t of tweets) {
        t.counterToUpdate = await updateTweet(t);
    }

    await setProcessedEntities();

    await updateEntities(tweets);

    await writeTopEntitiesToDisk();

    console.log("Finished processing tweets", new Date());

};

const updateTweet = async (tweet: Tweet) => {

    let count = tweet.public_metrics.retweet_count + tweet.public_metrics.quote_count;

    const prev: any = db.prepare('select * from tweets where id = ?').get(tweet.id);

    if (!prev) {
        db.prepare('insert into tweets values (?,?)').run(tweet.id, count);
        return count;
    } else {
        db.prepare('update tweets set count = ? where id = ?').run(count, tweet.id);
        return count - prev.count;
    }

};

const setProcessedEntities = async () => {
    console.log('setProcessedEntities');
    return db.prepare('update entities set processed = count, lastUpdateTime = ? where not IFNULL(processed, -1) = count').run(new Date().toUTCString());
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

    const entitiesStatement = db.prepare('Insert INTO entities(type,name,count,lastUpdateTime) values (?,?,?,?)\n' +
        'ON CONFLICT (type,name) DO UPDATE SET count = count + ?, lastUpdateTime = ?');

    db.transaction((entities:Array<Entity>) => {
        entities.forEach(entity => {
            entitiesStatement.run(
                entity.type,
                entity.name,
                entity.count,
                entity.lastUpdateTime.toUTCString(),
                entity.count,
                entity.lastUpdateTime.toUTCString()
            );
        });
    })(entities);

    console.log("done updateEntities");

};

const writeTopEntitiesToDisk = async () => {

    const topEntities = await fetchTopEntities();

    fs.writeFileSync('top-entities.json', JSON.stringify(topEntities), 'utf8');

};

const fetchTopEntities = async (): Promise<EntitiesResult> => {

    const hashtags = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(EntityType.HASHTAG);
    const cashtags = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(EntityType.CASHHASH);
    const mentions = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(EntityType.MENTION);
    const urls = db.prepare('select processed, count, name, lastUpdateTime from entities where type = ? order by processed desc, count desc limit 100').all(EntityType.URL);

    return {
        hashtags,
        cashtags,
        mentions,
        urls
    };

};

const truncateEntities = async () => {
    db.prepare('DELETE FROM entities').run();
};
