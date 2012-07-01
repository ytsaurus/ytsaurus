(function($){

    var RequestsMaster = function(cfg){
        isStarted = false;
        cfg = cfg || {};
        var that = this,
            requests = [],
            interval = false,
            prefix = cfg.prefix || "RequestsMaster";

        
        this.to = 10000;

        function getEventName(url){
            var pos = $.inArray(url, requests);
            if (pos === -1){
                return false;
            }
            return prefix+"_"+pos;
            
        }


        function makeRequests(){
            var onSuccess = function(data, pos){
                    $(document).trigger(prefix+"_"+pos, [data]);
                },
                onError = function(data, pos){
                    $(document).trigger(prefix+"_"+pos);
                };
            $.each(requests, function(pos, url){
                $(document).trigger(prefix+"_"+pos+"_before");
                $.ajax({
                    url: url,
                    success: function(data){
                        if (typeof data == "string"){
                            data = $.parseJSON(data);
                        }
                        onSuccess(data, pos);
                    },
                    error: function(data){onError(data, pos)}
                });
            })
        }

        this.addURL = function(url){
            var pos = $.inArray(url, requests);
            if (pos === -1){
                pos = requests.length;
                requests.push(url);
            }
            return prefix+"_"+pos;
        }

        this.start = function(){
            if (isStarted)
                return;
            this.stop();
            interval = window.setInterval(makeRequests, this.to);
            makeRequests();
            isStarted = true;
        }

        this.stop = function(){
            try {
                window.clearInterval(interval);
            } catch (e) {
            }
            isStarted = false;
        }

        this.isStarted = function(){
            return isStarted;
        }
    },
        rm = new RequestsMaster();


    var YTMonitorAbstractBlock = function(holder){
        var template = "";
        
        this.redraw = function(){
            // should be redefined
        }
                         
        this.addHandler = function(url, onstart, onresult){
            // handlers for the interesting events
            var e = rm.addURL(url);
            $(document).bind(e+"_before", function(){onstart && onstart()});
            $(document).bind(e, function(evt, data){onresult && onresult(data)});
        }
        
        this.init = function(){
            console.log("Init function is not defined for module " + (""+this.constructor)); 
            console.dir(this);
        }

        this.getOrchidURL = function(address, path){
            return "http://"+address+"/orchid"+path.replace("//", "/");
        }

        this.getOrchidURLs = function(path){
            var urls = [];
            $.each(YTM.config.currentCluster.masters, function(){
                urls.push("http://"+this+"/orchid"+path.replace("//", "/"));
            });
            return urls;
        }

        this.getCypressURL = function(path){
            return "http://" + YTM.config.currentCluster.proxy + "/api/get?path=" + encodeURIComponent(path);
        }

        this.stopRM = function(){
            rm.stop();
        }
    }

    YTMonitor = window.YTMonitor || {};
    YTMonitor.modules = YTMonitor.modules || {};
    var YTM = YTMonitor;
    
    YTM.init = function(){
        $('.yt-monitor-container').each(function(){
            var container = $(this),
                blockType = container.attr('data-module');
            try {
                if (YTM.modules[blockType]){
                    var inst = new YTM.modules[blockType](container);
                    inst.init();
                }
            } catch(e) {
                console.log(e);
                return;
            }
        });
    }

    YTMonitor.addModule = function(name, module){
        var ytmab = new YTMonitorAbstractBlock();
        module.prototype = ytmab;
        YTMonitor.modules = YTMonitor.modules || {};
        YTMonitor.modules[name] = module;
    };

    function initClusterSelector() {
        var selector = $('.yt-cluster-selector').empty();
        $.each(YTM.config.clusters, function(){ //YTM.config.currentCluster
            var item = $('<li>'+
                '    <a href="#">' +
                this.name + '<br/>&nbsp;' +
                this.masters.join('<br/>&nbsp;') + 
                '    </a>' +
                '</li>').appendTo(selector);
                if (this.name == YTM.config.currentCluster.name){
                    item.addClass("active");
                } else {
                    var name = this.name;
                    item.bind('click', function(){
                        setCookie('cluster', name);
                        window.location.reload();
                    });
                }
        });
    }

    
    $(function(){
        var currentClusterName = getCookie('cluster');
        $.each(YTM.config.clusters, function(){
            if (this.name == currentClusterName){
                YTM.config.currentCluster = this;
            }
        });

        $('.yt-monitor-container').each(function(){
            var module = $(this).attr('data-module');
            if (!module || !(module in YTMonitor.modules)) {
                return;
            }

        });
        YTM.config.currentCluster = YTM.config.currentCluster || YTM.config.clusters[0];
        
        YTM.init();
        rm.to = YTM.config.requestsTimeout * 1000 || rm.to;
        rm.start();
        initClusterSelector();
    });


})(jQuery)


