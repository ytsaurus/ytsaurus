(function(){ // for modules definishion

    var Masters = function(container){
        var module = this,
            servers = YTMonitor.config.currentCluster.masters;

        this.init = function(){
            var urls = module.getOrchidURLs("//monitoring/meta_state");
            $.each(servers, function(){
                var master = this,
                    url = module.getOrchidURL(master, "//monitoring/meta_state");
                module.addHandler(url, 
                    function(){onReq(master.split(".")[0])},
                    function(data){onRes(master.split(".")[0], data)});
            });
            fillContainer();
        }

        function fillContainer(){
            var rows = [];
            $.each(servers, function(){
                rows.push(
                    '        <tr class="'+this.split(".")[0]+'">'+
                    '            <td style="width: 8em;">'+
                    '                <span class="label yt-v-master-status hide" style="display: inline;"></span>'+
                    '            </td>'+
                    '            <td class="yt-v-master-name">'+this+'</td>'+
                    '        </tr>')});
            var html = '<table class="table">'+
                '    <tbody>'+
                rows.join("") + 
                '    </tbody>'+
                '</table>';
            container.html(html);
        }

        function onRes(master, data){
            var row = $("."+master, container).css('opacity', '1'),
                status = data && data.status || 'unreachable';
            $(".yt-v-master-status", row).text(status.toUpperCase());
            switch (data && data.status){
                case 'following':
                    row.addClass('notice');
                    $(".yt-v-master-status", row).text('FOLLOWING');
                    break;
                case 'leading':
                    row.addClass('success');
                    $(".yt-v-master-status", row).text('LEADING');
                    break;
                case 'elections':
                    row.addClass('warning');
                    break;
                default:
                    row.addClass('important');
                    break;
            }
        }

        function onReq(master){
            $(container).find("."+master).css('opacity', '0.3');
        }

        $.bind('YTMonitorDataUpdated', function(){console.log(111)});
    }


    YTMonitor.addModule("Masters", Masters);
})()
