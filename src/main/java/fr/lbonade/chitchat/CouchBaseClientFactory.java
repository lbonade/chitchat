package fr.lbonade.chitchat;

import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

/**
 * Created by lbonade on 19/07/2014.
 */
public class CouchBaseClientFactory {

    private static CouchbaseClient client = null;

    public static CouchbaseClient getInstance() throws IOException, URISyntaxException {
        synchronized (CouchBaseClientFactory.class) {
            if (client == null) {
                    client = new CouchbaseClient(Arrays.asList(new URI("http://localhost:8091/pools")), "chitchat", "");
            }
        }
        return client;
    }

}
