package fr.lbonade.chitchat.grizzly;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import fr.lbonade.chitchat.CouchBaseClientFactory;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.util.CharsetUtil;
import org.glassfish.grizzly.WriteHandler;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.io.NIOOutputStream;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by lbonade on 19/07/2014.
 */
public class LatestHandler extends HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(LatestHandler.class);

    @Override
    public void service(final Request request, final Response response) throws Exception {
        response.setBufferSize(0);
        if(request.getMethod() != Method.GET) {
            response.setStatus(HttpStatus.BAD_REQUEST_400.getStatusCode(), "Get required");
            return;
        }

        String author = request.getRequestURI().substring("/chitchat/latest/".length());
        logger.info("uri : "+request.getRequestURI()+"::"+author);
        final NIOOutputStream out = response.getNIOOutputStream();
        response.suspend();
        final CouchbaseClient cbc = CouchBaseClientFactory.getInstance();
        final Query queryLatest = new Query();
        final View viewLatest = cbc.getView("documents", "latest");
        response.setStatus(HttpStatus.OK_200);
        response.setCharacterEncoding("UTF-8");
        response.setContentType("application/json");
        ComplexKey key = ComplexKey.of(author);
        queryLatest.setRangeStart(key);
        queryLatest.setRangeEnd(key);
        queryLatest.setIncludeDocs(false);
        queryLatest.setGroup(true);
        queryLatest.setGroupLevel(1);
        final ViewResponse viewResponse = cbc.query(viewLatest, queryLatest);
        final Iterator<ViewRow> iterator = viewResponse.iterator();
        out.notifyCanWrite(new IteratorWriter<ViewRow>(iterator, out, response, (r) -> {return r.getValue();}));

    }
}
