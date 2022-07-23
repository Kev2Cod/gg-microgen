/**
 * Responds to HTTP request.
 *
 * @param { app, context, callback }
 * app { getRequester, getPublisher }
 * context { body, cookies, method, params, query, headers }
 * callback(error, response) 
 */

const { Client } = require('@elastic/elasticsearch')
const { createClient } = require("redis")

const es = new Client({
    node: 'http://10.207.26.1:9200',
})

const redis = createClient({ url: 'redis://kevin:secret@localhost:6379' })

const ES_INDEX = 'dev_gg_survey_responses_all_field_new'
const SIZE = 10
const EXPIRED = 3600
const initialKey = 'gg[1-9]'
const prefixKey = 'gg'

console.log("ES_INDEX: ", ES_INDEX)
console.log("SIZE: ", SIZE)
console.log("EXPIRED: ", EXPIRED)

function generateNextToken(sort) {
    nextSearch = [sort[0], sort[1] + SIZE]
    return nextSearch
}

const insertToRedis = async (searchAfter) => {
    try {
        const pit = await es.openPointInTime({ index: ES_INDEX, keep_alive: '1m' })

        const reqArg = {
            size: SIZE,
            query: {
                match_all: {}
            },
            pit: {
                id: pit.id,
                keep_alive: '1m'
            },
            sort: [
                {
                    "_score": "desc"
                },
                {
                    "_shard_doc": "asc"
                }
            ],
        }

        let key
        let value
        let allData = []

        // First time to get data from ES
        const firstSearch = await es.search(reqArg)
        searchAfter = firstSearch.hits.hits.at(-1).sort
        key = searchAfter.join('-')
        value = JSON.stringify({ data: firstSearch.hits.hits.map(val => val._source), nextToken: generateNextToken(searchAfter) })

        allData.push(JSON.stringify({ data: firstSearch.hits.hits.map(val => val._source) }))

        await redis.set(`${prefixKey}[${key}]`, value, { EX: EXPIRED })

        // Function get data from ES and send to redis
        const sendAllEsToRedis = async (page) => {

            const nextSearch = await es.search({
                ...reqArg,
                search_after: [nextToken[0], nextToken[1] - SIZE]
            })

            key = nextToken.join('-')
            value = JSON.stringify(nextSearch.hits.hits.map(val => val._source))
            allData.push(value)
            console.log("Send to Redis being proses...")
            await redis.set(`${prefixKey}[${key}]`, JSON.stringify(allData.join(",")), { EX: EXPIRED })
            console.log("Send to Redis Success!")

            nextToken = generateNextToken(nextSearch.hits.hits.at(-1).sort);
            console.log("Next Token: ", nextToken)
            console.log(' - selesai melakukan insert data')
        }

        let nextToken = generateNextToken(searchAfter);

        var i = 1;
        function myLoop() {
            setTimeout(function () {
                console.log('LOOPINGG 1 Minutes');
                i++;
                if (i < 8) {
                    sendAllEsToRedis(nextToken)
                    myLoop();
                }
            }, 30000)
        }
        myLoop();

    } catch (error) {
        console.log(error)
        return error
    }
}

exports.allEsToRedis = async (req, res) => {
    console.log("Function execute all is Running..!")
    // check if redis is connected
    try {
        await redis.ping()
    } catch (e) {
        await redis.connect()
    }

    let result
    const nextToken = req.headers['next_token'] || req.query['next_token']

    try {
        console.log(await redis.ping())

        if (!nextToken) {
            if (!await redis.exists(initialKey)) {
                await insertToRedis()
            }

            result = JSON.parse(await redis.get(initialKey))

        } else {
            const decodeToken = Buffer.from(nextToken, 'base64').toString('utf-8')

            if (!await redis.exists(`${prefixKey}[${decodeToken}]`)) {
                await insertToRedis()
            }
            result = JSON.parse(await redis.get(`${prefixKey}[${decodeToken}]`))
        }

        const response = {
            statusCode: 200,
            result
        }

        //  callback(null, response);
        res.send(response)

    } catch (error) {
        //  return callback(null, error)
        console.log(error)
        return res.send(error)
    }
}
