Overview
===
This library is binding for C++ library with CGO.
It provides ability to operate with TVM. Library is fast enough to get or check tickets for every request without burning CPU.

You can find some examples in [godoc](https://godoc.yandex-team.ru/pkg/a.yandex-team.ru/library/go/yandex/tvm/tvmauth/).

[Home page of project](https://wiki.yandex-team.ru/passport/tvm2/)

You can ask questions: [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)

Tvm Client
===
It is based on `Client`. Don't forget to collect logs from client.

If you don't need an instance of client anymore but your process would keep running, please `Destroy()` this instance.
___
`Client` allowes:
1. `GetServiceTicketFor{Alias,ID}()` - to fetch ServiceTicket for outgoing request
2. `CheckServiceTicket()` - to check ServiceTicket from incoming request
3. `CheckUserTicket()` - to check UserTicket from incoming request

All methods are thread-safe.
___
You should check status of client with `GetStatus()`:
* `ClientOK` - nothing to do here
* `ClientWarning` - **you should trigger your monitoring alert**

      Normal operation of TvmClient is still possible but there are problems with refreshing cache, so it is expiring.
      Is tvm-api.yandex.net accessible?
      Have you changed your TVM-secret or your backend (dst) deleted its TVM-client?

* `ClientError` - **you should trigger your monitoring alert and close this instance for user-traffic**

      TvmClient's cache is already invalid (expired) or soon will be: you can't check valid CheckedServiceTicket or be authenticated by your backends (dsts)

___
Constructor creates system thread for refreshing cache - so do not fork your proccess after creating `Client` instance. `New{Tool,API}Client` leads to network I/O. Other methods always use memory.

Error from `New{Tool,API}Client` is `tvm.Error` with field `Retriable`:
* `true` - maybe some network trouble: you can try to create client one more time.
* `false` - settings are bad: fix them.

Other methods can return error only if you try to use unconfigured abilities (for example, you try to get ServiceTicket for some dst but you didn't configured it in settings).
___
You can choose way for fetching data for your service operation:
* http://localhost:{port}/tvm - recomended way
* https://tvm-api.yandex.net

TvmTool
------------
`NewToolClient` uses local http-interface to get state. This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
See more: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.

`NewToolClient` fetches configuration from tvmtool, so you need only to tell client how to connect to it and tell which alias of tvm id should be used for this `Client` instance.

TvmApi
------------
`NewAPIClient`.
First of all: please use `TvmAPISettings.DiskCacheDir` - it provides reliability for your service and for tvm-api.
Please check restrictions of this method.
