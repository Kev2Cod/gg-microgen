var express = require('express');
var router = express.Router();

const { esToRedis } = require('../controllers/execute');
const { esToRedisWithEndpoint } = require('../controllers/executeAllWithEndpoint');

router.get('/execute', esToRedis)
router.get('/all-execute', esToRedisWithEndpoint)



module.exports = router;
