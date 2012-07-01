(function(){ // for modules definishion

    var NodeInfo = function(container){
        var module = this;

        this.init = function(){
            var url = module.getCypressURL("//sys/holders/@");

            fillContainer();
            module.addHandler(url, 
                    function(){onReq()},
                    function(data){onRes(data)});

        }

        function fillContainer(){
            var  html = '    <h3>Место</h3>' +
                '    <div style="margin-bottom: 0" class="progress progress-info">' + 
                '        <div class="space_bar bar" style="width: 0%;"></div>' +
                '    </div>' +
                '    <p><strong>Доступно</strong>: <span class="available_space"></span><br/>' +
                '    <strong>Всего</strong>: <span class="total_space"></span></p>' +
                '    <h3>Ноды</h3>' +
                '    <div style="margin-bottom: 0" class="progress progress-info">' + 
                '        <div class="nodes_bar bar" style="width: 0%;"></div>' +
                '    </div>' +
                '    <p><strong>Чанки</strong>: <span class="chunks"></span></p>';
            container.html(html);
        }

        function onRes(data){
            function toTb(bites){
                return Math.round((bites / 1099511627776) * 100) / 100
            }
            if (data){
                if (data.used_space){
                    $('.space_bar', container).css('width', (data.used_space / data.available_space) * 100 + "%");
                    $('.available_space', container).html(toTb(data.available_space - data.used_space) + "Тб <small>(" + humanizeNumber(data.available_space - data.used_space, "'") + ' байт)</small>');
                    $('.total_space', container).html(toTb(data.available_space) + "Тб <small>(" + humanizeNumber(data.available_space, "'") + ' байт)</small>');
                }
                if (data.count) {
                    $('.nodes_bar', container).css('width', (data.online_holder_count / data.count) * 100 + "%")
                        .text(data.online_holder_count + " / " + data.count);
                }
                $('.chunks', container).text(data.chunk_count);
                $(container).animate({'opacity': '1'}, 500);
            }
        }

        function onReq(){
            $(container).css('opacity', '0.3');
        }
    }


    YTMonitor.addModule("NodeInfo", NodeInfo);
})()
