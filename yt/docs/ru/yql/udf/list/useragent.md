# UserAgent UDF
``` yql
UserAgent::Parse(String?) -> Struct<BrowserName:String?,isBrowser:Bool,...>
```
С помощью библиотеки [uatraits]({{source-root}}/metrika/uatraits) определяет и возвращает набор свойств переданного User Agent.

На момент написания возвращает структуру со следующими полями (пояснения по их значениям см. [здесь](https://nda.ya.ru/t/rbVJNO4m7Es8jd)):

* `String?`:
    * BrowserName
    * BrowserVersion
    * BrowserEngine
    * BrowserEngineVersion
    * BrowserBase
    * BrowserShell
    * BrowserShellVersion
    * OSName
    * OSFamily
    * OSVersion
    * DeviceName
    * DeviceModel
    * DeviceVendor
    * YandexBar
    * YandexBarVersion
    * MailRuSputnik
    * MailRuSputnikVersion
    * MailRuAgent
    * MailRuAgentVersion
    * GoogleToolBar
    * GoogleToolBarVersion
* `Bool`:
    * isEmulator
    * isBrowser
    * isBeta
    * isMobile
    * isTablet
    * isTouch
    * isWAP
    * isRobot
    * isTV
    * x64
    * MultiTouch
    * historySupport
    * localStorageSupport

#### Примеры

``` yql
SELECT UserAgent::Parse("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1104.221 YaBrowser/1.5.1104.221 Safari/537.4").BrowserName; -- ["YandexBrowser"]
SELECT UserAgent::Parse("Mozilla/5.0 (Linux; Android 4.0.3; U9500 Build/HuaweiU9500) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 YaBrowser/14.2.1700.12147.00 Mobile Safari/537.36").isMobile; -- true
```
