(function(){ // for modules definishion

    var NodesMonitor = function(container){
        var module = this;

        this.init = function(){
            var url = module.getCypressURL("//sys/holders/@");

            fillContainer();
            module.addHandler(url, 
                    function(){onReq()},
                    function(data){onRes(data)});

        }

        function fillContainer(){
            var  html = '<div class="nodes_monitor_holder">' +
                '</div>';
            container.html(html);
        }

        function onRes(data){
            if (data){
                var allNodes = [],
                    states = {},
                    fields = ['offline', 'registered', 'online', 'unconfirmed'];

                $.each(fields, function(){
                    var state = this;
                    $.merge(allNodes, data[state]);
                    $.each(data[state], function(){
                        states[this] = state;
                    })
                    
                });
                allNodes.sort();
                $.each(allNodes, function(){
                    var className = "nodes_monitor__" + this.split('.')[0],
                        elem = $("." + className, container);
                    if (!elem.length) {
                        elem = $('<div class="' + className + ' nodes_monitor_elem"></div>').appendTo($('.nodes_monitor_holder', container));
                    }
                    
                    switch ("" + states[this]){
                        case 'online':
                            elem.removeClass('nodes_monitor_elem_err')
                                .addClass('nodes_monitor_elem_ok')
                                .removeClass('nodes_monitor_active');
                            break;
                        case 'offline':
                        case 'registered':
                        case 'unconfirmed':
                            elem.removeClass('nodes_monitor_elem_ok')
                                .addClass('nodes_monitor_elem_err')
                                .removeClass('nodes_monitor_active');
                            break;
                        default:
                            break;
                    }
                    elem.attr('title', this + " - " + states[this]);
                });
            }
        }

        function onReq(){
            $('.nodes_monitor_elem', container).addClass('nodes_monitor_active');
        }
    }


    YTMonitor.addModule("NodesMonitor", NodesMonitor);
})()
