REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar
request            = LOAD '$input' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader AS (ip:chararray, l: chararray, remote_user: chararray, request_datetime: chararray, method: chararray, request_path: chararray, scheme: chararray, status_code: int, content_length: chararray, referer: chararray, user_agent: chararray);
html_request       = FILTER request BY method == 'GET' AND request_path matches '(?:/[^ ]*)?/(?:[^/]+\\.html)?' AND (status_code == 200 OR status_code == 304);
fixed_html_request = FOREACH html_request GENERATE ip, (request_path matches '.*/$' ? CONCAT(request_path, 'index.html') : request_path) AS request_path, org.apache.pig.piggybank.evaluation.string.REPLACE(request_datetime, '^(\\d{2})/(\\w{3})/(\\d{4}).*$', '$3/$2/$1') AS request_date;
grouped_requests   = GROUP fixed_html_request BY (ip, request_path, request_date);
count              = FOREACH grouped_requests GENERATE FLATTEN(group), COUNT(fixed_html_request);
STORE count INTO '$output';
