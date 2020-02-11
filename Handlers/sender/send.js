module.exports =async function (emmiter) {
    emmiter.on("sendData", async function(msg1,msg2,msg3) {
        try {
            let callRest = funclib['callRest']
            let res = await callRest("http://localhost:5000/send", msg3['data'])
            console.log(msg3['data'])
            console.log(res)
        }
        catch (err) {
            console.log(err)
        }
    })
}
