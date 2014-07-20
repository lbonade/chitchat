package fr.lbonade.chitchat.grizzly;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import fr.lbonade.chitchat.CouchBaseClientFactory;
import org.glassfish.grizzly.WriteHandler;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.io.NIOOutputStream;
import org.glassfish.grizzly.http.io.NIOWriter;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Created by lbonade on 19/07/2014.
 */
public class ThreadHandler extends HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(ThreadHandler.class);

    @Override
    public void service(final Request request, final Response response) throws Exception {
        // Disable internal Response buffering
        response.setBufferSize(0);
        if(request.getMethod() != Method.GET) {
            response.setStatus(HttpStatus.BAD_REQUEST_400.getStatusCode(), "Get required");
            return;
        }
        String thread = request.getRequestURI().substring("/chitchat/thread/".length());
        logger.info("uri : " + request.getRequestURI() + "::" + thread);
        response.setCharacterEncoding("UTF-8");
        response.setContentType("application/json");
        final NIOOutputStream out = response.getNIOOutputStream();

        final CouchbaseClient cbc = CouchBaseClientFactory.getInstance();
        ComplexKey key = ComplexKey.of(Long.parseLong(thread));
        final Query queryThread = new Query();
        final View viewThread = cbc.getView("documents", "threads");
        queryThread.setIncludeDocs(true);
        queryThread.setRangeStart(key);
        queryThread.setRangeEnd(key);
        final ViewResponse viewResponse = cbc.query(viewThread, queryThread);
        final Iterator<ViewRow> iterator = viewResponse.iterator();
        //response.suspend();
        out.notifyCanWrite(new IteratorWriter<ViewRow>(iterator, out, response, (r) -> {return (String)r.getDocument();}));

    }
}
