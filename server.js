var express = require('express');
var fs = require('fs');
var app = express();
var bodyParser = require('body-parser');

app.use(bodyParser.json()); //body-parser 解析json格式数据
app.use(bodyParser.urlencoded({ //此项必须在 bodyParser.json 下面,为参数编码
	extended: true
}));
//  主页输出 "Hello World"
app.get('/', function(req, res) {
	console.log("主页 GET 请求");
	res.send('Hello GET');
})


//  POST 请求
app.post('/pushData', function(req, res) {
	var json_data = req.body;
	var _Buffer = new Buffer(JSON.stringify(json_data.data));
	fs.writeFile('./tiehang/' + json_data.busId + ".txt", _Buffer, {
		encoding: "utf8",
		flag: "a"
	}, function(err, bytesRead, buffer) {
		if (err) throw err;
	})
	res.send(true);
});


var server = app.listen(8081, function() {

	var host = server.address().address
	var port = server.address().port

	console.log("应用实例，访问地址为 http://%s:%s", host, port)

})

app.get('/index.htm', function(req, res) {
	res.sendFile(__dirname + "/" + "index.htm");
})

app.use(express.static('www'));