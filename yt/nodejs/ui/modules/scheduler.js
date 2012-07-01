(function(){ // for modules definishion

    var Scheduler = function(container){
        var scheduler = this,
            servers = YTMonitor.config.currentCluster.masters,
            scheduler_requests = Array(2);

        this.className = "Masters";


        this.init = function(){
            var url = scheduler.getCypressURL("//sys/scheduler/@");

            scheduler.addHandler(url, 
                    function(){onReq(0)},
                    function(data){onRes(0, data)});

            url = scheduler.getCypressURL("//sys/scheduler/lock/@");
            scheduler.addHandler(url, 
                    function(){onReq(1)},
                    function(data){onRes(1, data)});

            fillContainer();
            subscribeMasters();
        }

        function fillContainer(){
            var html = '<table class="table">'+
                '    <tbody>'+
                '        <tr class="sch_quorum">'+
                '            <td style="width: 8em;">'+
                '                <span class="label yt-v-master-status hide" style="display: inline;">Кворум</span>'+
                '            </td>'+
                '            <td class="yt-v-master-name"></td>'+
                '        </tr>'+
                '        <tr class="sch_schedule">'+
                '            <td style="width: 8em;">'+
                '                <span class="label yt-v-master-status hide" style="display: inline;">Шедулер</span>'+
                '            </td>'+
                '            <td class="yt-v-master-name"></td>'+
                '        </tr>'+
                '    </tbody>'+
                '</table>';
            container.html(html);
        }

        function subscribeMasters(){
            var servers = YTMonitor.config.currentCluster.masters;
            $.each(servers, function(){
                var master = this,
                    url = scheduler.getOrchidURL(master, "//monitoring/meta_state");
                scheduler.addHandler(url, 
                    function(){$('.sch_quorum', container).css('opacity', '0.3')},
                    updateQuorum
                );
            });
            
        }

        function updateQuorum(data){
            if (!data){
                $('.sch_quorum', container)
                    .removeClass("success")
                    .addClass("important")
                    .animate({'opacity': '1'}, 500);
                return;
            }
            if (data && data.status && data.status !== 'following'){
                var row = $('.sch_quorum', container).animate({'opacity': '1'}, 500);;
                switch (data.status){
                    case 'leading':
                        if (data.has_quorum == "true"){
                            row.addClass("success")
                                .removeClass("important");
                            $('.yt-v-master-name', row).text(data.version);
                            break;
                        }
                    case 'election':
                    default: 
                        row.addClass("important")
                            .removeClass("success");
                        break;
                }
            }
        }

        function onRes(pos, data){
            var row = $(".sch_schedule", container).animate({'opacity': '1'}, 500);
            
            scheduler_requests[pos] = data || false;
            row.removeClass('success')
                .removeClass('important')
                .removeClass('warning');

            if (scheduler_requests[0] && scheduler_requests[1]){
                $('.yt-v-master-name', row).text(scheduler_requests[0].address || '-');
                row.addClass((scheduler_requests[1].lock_ids && scheduler_requests[1].lock_ids.length ? "success" : "warning"));
            } else {
                $('.yt-v-master-name', row).text('');
                row.addClass('important');
            }
        }

        function onReq(pos){
            $(".sch_schedule", container).css('opacity', '0.3');
            scheduler_requests[pos] = false;
        }
    }


    YTMonitor.addModule("Scheduler", Scheduler);
})()
