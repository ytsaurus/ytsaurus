/*
COOKIES
*/

function setCookie(cookie_name, value, expire)
{
    var expire_date = new Date();
    
    expire_date.setDate(expire_date.getDate() + expire);
    document.cookie = (cookie_name + "=" + escape(value) + ((expire == null) ? "" : ";expires=" + expire_date.toGMTString()));

    return true;
}


function getCookie(cookie_name)
{
    if (document.cookie.length > 0) {
        cookie_start = document.cookie.indexOf(cookie_name + "=");
        
        if (cookie_start != -1) { 
            cookie_start = ((cookie_start + cookie_name.length) + 1); 
            cookie_end   = document.cookie.indexOf(";", cookie_start);
            
            if ( cookie_end == -1) {
                cookie_end = document.cookie.length;
            }
            
            return unescape(document.cookie.substring(cookie_start, cookie_end));
        } 
    }
    
    return false;
}


/*
*/

function humanizeNumber(num, delimiter){
    delimiter = delimiter || ",";
    var re = new RegExp('(\\d)(\\d{3})(\\'+delimiter+'|$)', "g");
    return re.test(""+num) ? humanizeNumber(("" + num).replace(re, function(m,m1){ 
            return arguments[1]+delimiter+arguments[2];
        }), delimiter) : num;
}