package org.apache.flink.connector.databend.util;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class DatabendJdbcUtil {
    private static final Pattern HTTP_PORT_PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");

    public static int getActualHttpPort(String host, int port) throws SQLException {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet((new URIBuilder())
                    .setScheme("http")
                    .setHost(host)
                    .setPort(port)
                    .build());
            HttpResponse response = httpclient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String raw = EntityUtils.toString(response.getEntity());
                Matcher matcher = HTTP_PORT_PATTERN.matcher(raw);
                if (matcher.find()) {
                    return Integer.parseInt(matcher.group("port"));
                }
                throw new SQLException("Cannot query Databend http port.");
            }

            return port;
        } catch (Throwable throwable) {
            throw new SQLException("Cannot connect to Databend server using HTTP.", throwable);
        }
    }
}
