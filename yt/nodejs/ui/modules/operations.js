(function(){ // for modules definishion

    var Operations = function(container){
        var module = this,
            proxy  = YTMonitor.config.currentCluster.proxy;

        this.operation = false;

        this.init = function(){
            var m = window.location.search.match(/(\?|\&)id=(.*)(\&|$)/);
            if (m && m[2]) { // show the operation details
                fillContainer();
                this.operation = m[2];
                var url = 'http://' + proxy + '/api/get?path=//sys/operations/%22' + m[2] + '%22&with_attributes=true';
                module.addHandler(url, 
                    function(){},
                    drawOperationDetails);
                url = 'http://' + proxy + '/api/get?path=//sys/operations/%22' + m[2] + '%22/jobs&with_attributes=true';
                module.addHandler(url, 
                    function(){},
                    drawJobs);
            } else { // show the operations list
                var url = 'http://' + proxy + '/api/get?path=//sys/operations&with_attributes=true';
                module.addHandler(url, 
                    function(){},
                    drawOperationsList);
            }
        }

        function drawOperationsList(data){
            module.stopRM();
            if (!data || !data['$value']){
                container.html('<div class="alert alert-error"><strong>Oh no!</strong> The data cannot be retrieved from the server.</div>');
                return;
            }
            this.operationsList = data['$value'];
            var html = '  <div class="page-header">' + 
                '    <h1>Список операций</h1>' + 
                '  </div>' + 
                '  <div class="row">' + 
                '    <div class="span12">' + 
                '      <table class="table table-bordered table-striped">' + 
                '        <thead>' + 
                '          <tr>' + 
                '            <th class="span1">&nbsp;</th>' + 
                '            <th class="span4">GUID</th>' + 
                '            <th class="span2">Начало работы</th>' + 
                '            <th class="span2">Конец работы</th>' + 
                '            <th class="span3">Прогресс</th>' + 
                '          </tr>' + 
                '        </thead>' + 
                '        <tbody>',
                rows = [];
            $.each(data['$value'], function(k, v){
                var progress = Math.floor(v['$attributes'].progress.jobs.completed / (v['$attributes'].progress.jobs.total || 1) * 100),
                    icon;
                switch (v['$attributes'].state) {
                    case 'running':
                        icon = '<span class="badge badge-info"><i class="icon-white icon-play"></i></span>';
                        break;
                    case 'completed':
                        icon = '<span class="badge badge-success"><i class="icon-white icon-ok"></i></span>';
                        break;
                    case 'failed':
                        icon = '<span class="badge badge-important"><i class="icon-white icon-remove"></i></span>';
                        break;
                    default:
                        icon = '<span class="badge"><i class="icon-white icon-pause"></i></span>';
                        break;
                }
                var row = '          <tr>' +
                    '            <td style="text-align: center;">' + icon + '</td>' +
                    '            <td><a class="yt-operations-item" href="?id=' + k + '" data-operation-id="' + k + '"><code>' + k + '</code></a></td>' +
                    '            <td>' + v['$attributes'].start_time + '</td>' +
                    '            <td>' + (v['$attributes'].end_time  || "&mdash;") + '</td>' +
                    '            <td>' +
                    '              <div class="progress progress-success" style="margin-bottom: 0;">' +
                    '                <div class="bar" style="width: ' + progress + '%;"><span>' + v['$attributes'].progress.jobs.completed + ' / ' + v['$attributes'].progress.jobs.total + '</span></div>' +
                    '              </div>' +
                    '            </td>' +
                    '          </tr>';
                rows.push(row);
            });
            html += rows.join('');
            html += '        </tbody>' +
                '      </table>' +
                '    </div>';
            container.html(html);
/*
            $('.yt-operations-item', container).bind('click', function(e){
                e.preventDefault();
            });
*/
            return html;
        }

        function drawOperationDetails(data){
            module.stopRM();
            if (!data){
                $('.yt-operations-op', container).html('<div class="alert alert-error"><strong>Oh no!</strong> Cannot load operation details from the server.</div>');
                return;
            }
            if (!data){
                $('.yt-operations-op', container).html('<div class="alert alert-error"><strong>Oh no!</strong> Cannot find the specified operation details.</div>');
                return;
            }
            var tpl = $('.templates .yt-operations-details').clone(),
                icon = '';
            switch (data['$attributes'].state) {
                case 'running':
                    icon = '<span class="label label-info"><i class="icon-white icon-play"></i>' + data['$attributes'].state + '</span>';
                    break;
                case 'completed':
                    icon = '<span class="label label-success"><i class="icon-white icon-ok"></i>' + data['$attributes'].state + '</span>';
                    break;
                case 'failed':
                    icon = '<span class="label label-important"><i class="icon-white icon-remove"></i>' + data['$attributes'].state + '</span>';
                    break;
                default:
                    icon = '<span class="label"><i class="icon-white icon-pause"></i>' + data['$attributes'].state + '</span>';
                    break;
            }
            var num = 123123;
            var inputTables = [],
                outputTables = [];
            $.each(data['$attributes'].spec.input_table_paths, function(){inputTables.push('<li><code>'+this+'</code></li>');});
            if (data['$attributes'].spec.output_table_path) {
                outputTables.push(data['$attributes'].spec.output_table_path);
            } else {
                $.each(data['$attributes'].spec.output_table_paths, function(){outputTables.push('<li><code>'+this+'</code></li>');});
            }


            $('.yt-operations-id', tpl).html(module.operation);
            $('.yt-operations-ico', tpl).html(icon);
            $('.yt-operations-jobs', tpl).html(data['$attributes'].progress.jobs.completed + ' / ' + data['$attributes'].progress.jobs.total);
            $('.yt-operations-jobs_start', tpl).html(data['$attributes'].start_time || "&mdash;");
            $('.yt-operations-input_tables', tpl).html(inputTables.join(""));
            $('.yt-operations-output_tables', tpl).html(outputTables.join(""));
            $('.yt-operations-type', tpl).html(data['$attributes'].operation_type);
            $('.yt-operations-op', container).empty().append(tpl);
            if (data['$attributes'].spec.mapper && data['$attributes'].spec.mapper.command) {
                $('.yt-operations-command', container).text(data['$attributes'].spec.mapper.command);
            } else {
                $('.yt-operations-command', container).closest('.row').remove();
            }
        }

        function toTb(bites){
            return Math.round((bites / 1099511627776) * 100) / 100;
        }

        function formatDataSize(num){
            return '<p>' + toTb(num) + '&nbsp;Тб <small>(' + humanizeNumber(num, "'") + '&nbsp;байт)</small></p>';
        }

        function drawJobs(data){
            module.stopRM();
            if (!data){
                $('.yt-operations-jobs_block', container).html('<div class="alert alert-error"><strong>Oh no!</strong> Cannot load jobs list from the server.</div>');
            }
            data = data.$value;
            var html = '<div class="page-header">' +
                    '    <h1>Информация о джобах</h1>' +
                    '  </div>' +
                    '  <div class="row">' +
                    '    <div class="span12">' +
                    '      <table class="table table-striped table-bordered table-condensed">' +
                    '        <thead>' +
                    '          <th class="span1">#</th>' +
                    '          <th class="span1">Статус</th>' +
                    '          <th class="span4">Хост</th>' +
                    '          <th class="span2">Логи</th>' +
                    '        </thead>' +
                    '        <tbody>',
                rows = [],
                n = 0;
            $.each(data, function(k,v){
                var icon = '';
                n++;
                switch (v.$attributes.state) {
                    case 'running':
                        icon = '<span class="label label-info"><i class="icon-white icon-play"></i>' + v.$attributes.state + '</span>';
                        break;
                    case 'completed':
                        icon = '<span class="label label-success"><i class="icon-white icon-ok"></i>' + v.$attributes.state + '</span>';
                        break;
                    case 'failed':
                        icon = '<span class="label label-important"><i class="icon-white icon-remove"></i>' + v.$attributes.state + '</span>';
                        break;
                    default:
                        icon = '<span class="label"><i class="icon-white icon-pause"></i>' + v.$attributes.state + '</span>';
                        break;
                }

                rows.push('          <tr>' +
                    '            <td>' + k + '</td>' +
                    '            <td style="text-align: center;">' + icon + '</td>' +
                    '            <td>' + v.$attributes.address + '</td>' +
                    '            <td><a href="#">' + (v.$value.stderr ? '<a href="http://'+proxy+'/api/download?path=//sys/operations/%22'+module.operation+'%22/jobs/%22'+k+'%22/stderr">STDERR</a>' : '') + '</td>' +
                    '          </tr>');
            });

            html += rows.join('');

            html += '        </tbody>' +
                    '      </table>' +
                    '    </div>' +
                    '  </div>';

            $('.yt-operations-jobs_block', container).html(html)

        }

        function fillContainer(){
            var html = '<div class="yt-operations-op">Loading data...</div>';
            container.html(html);
            container.append($('<div class="yt-operations-jobs_block"></div>'));
        }

        function onRes(master, data){
            var row = $("."+master, container).css('opacity', '1'),
                status = data && data.status || 'unreachable';
            $(".yt-v-master-status", row).text(status.toUpperCase());
            switch (data && data.status){
                case 'following':
                    row.addClass('table-stripe-info').addClass('notice');
                    $(".yt-v-master-status", row).text('FOLLOWING');
                    break;
                case 'leading':
                    row.addClass('table-stripe-success').addClass('success');
                    $(".yt-v-master-status", row).text('LEADING');
                    break;
                case 'elections':
                    row.addClass('table-stripe-warning');
                    break;
                default:
                    row.addClass('table-stripe-important');
                    break;
            }
        }

        function onReq(master){
            $(container).find("."+master).css('opacity', '0.3');
        }

    }


    YTMonitor.addModule("Operations", Operations);
})()
