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
const axios = require('axios')

const es = new Client({
    node: 'http://10.207.26.1:9200',
})

const redis = createClient({ url: 'redis://admin:secret@localhost:6379' })

const ES_INDEX = 'dev_gg_survey_responses_all_field_new'
const SIZE = 10
// const EXPIRED = 3600
const prefixKey = 'gg'

function generateNextToken(sort) {
    nextSearch = `${sort[0]}-${sort[1] + SIZE}`
    return Buffer.from(nextSearch).toString('base64')
}

const insertToRedis = async (searchAfter) => {
    try {
        const pit = await es.openPointInTime({ index: ES_INDEX, keep_alive: '1m' })

        const reqArg = {
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

        if (!searchAfter) {
            const firstSearch = await es.search(reqArg)
            searchAfter = firstSearch.hits.hits.at(-1).sort
            key = searchAfter.join('-')
            value = JSON.stringify({ data: firstSearch.hits.hits.map(val => val._source), nextToken: generateNextToken(searchAfter) })
            await redis.set(`${prefixKey}[${key}]`, value)
            console.log(`Data send gg[${key}] to Redis success ✔ ...`)

            // axios.get(`http://localhost:3000/api/all-execute?next_token=${generateNextToken(searchAfter)}`).catch(err => console.log("Error: ", err))

            const decodeToken = Buffer.from(generateNextToken(searchAfter), 'base64').toString('utf-8')
            await insertToRedis(decodeToken.split('-')) 
            return
        } else {
            console.log(`Proses data gg[${searchAfter.join("-")}] Running`)
            const nextSearch = await es.search({
                ...reqArg,
                size: SIZE,
                search_after: [searchAfter[0], searchAfter[1] - SIZE]
            })

            if (nextSearch.hits.hits.length === 0) {
                console.log(' - Update All Data Success')
                return
            } else {
                searchAfter = nextSearch.hits.hits.at(-1).sort
                searchAfter = [searchAfter[0], searchAfter[1]]
                key = searchAfter.join('-')
                value = JSON.stringify({ data: nextSearch.hits.hits.map(val => val._source), nextToken: generateNextToken(searchAfter) })
                console.log("Data send Redis being process 🔃 ...")
                await redis.set(`${prefixKey}[${key}]`, value)
                console.log("Data send to Redis success ✔ ...")

                const decodeToken = Buffer.from(generateNextToken(searchAfter), 'base64').toString('utf-8')
                await insertToRedis(decodeToken.split('-')) 
                return
            }
        }

    } catch (error) {
        console.log(error)
        return error
    }
}

exports.esToRedisWithEndpoint = async (req, res) => {
    // check if redis is connected
    try {
        await redis.ping()
    } catch (e) {
        await redis.connect()
    }

    // let result
    const nextToken = req.headers['next_token'] || req.query['next_token']

    try {
        console.log(await redis.ping())

        if (!nextToken) {
            await insertToRedis()
        } else {
            const decodeToken = Buffer.from(nextToken, 'base64').toString('utf-8')
            await insertToRedis(decodeToken.split('-'))
        }

        const response = {
            statusCode: 200,
            status: "Update data running..."
        }

        //  callback(null, response);
        res.send(response)

    } catch (error) {
        //  return callback(null, error)
        console.log(error)
        return res.send(error)
    }
}
