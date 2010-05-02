REGISTER $jar
request            = LOAD '$input' USING st.happy_camper.pig.scala.aggregate.storage.CombinedLogLoader;
html_request       = FILTER request BY method == 'GET' AND request_path matches '(?:/[^ ]*)?/(?:[^/]+\\.html)?' AND (status_code == 200 OR status_code == 304);
fixed_html_request = FOREACH html_request GENERATE remote_host, (request_path matches '.*/$' ? CONCAT(request_path, 'index.html') : request_path) AS request_path, st.happy_camper.pig.scala.aggregate.evaluation.LongToDateFormat(requested_time, 'yyyy/MM/dd') AS request_date;
grouped_requests   = GROUP fixed_html_request BY (remote_host, request_path, request_date);
count              = FOREACH grouped_requests GENERATE FLATTEN(group), COUNT(fixed_html_request);
STORE count INTO '$output';
