package fr.lbonade.chitchat.grizzly;

import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpPacket;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.HttpResponsePacket;
import org.glassfish.grizzly.memory.Buffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.CharConversionException;
import java.io.IOException;

/**
 * Created by lbonade on 19/07/2014.
 */
public class HttpFilter extends BaseFilter {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final StringBuffer buffer = new StringBuffer(100);

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        // Get the incoming message as HttpContent
        final HttpContent httpContent = (HttpContent) ctx.getMessage();
        // Get HTTP request message header
        final HttpRequestPacket request = (HttpRequestPacket) httpContent.getHttpHeader();

        // Check if it's the last HTTP request chunk
        if (!httpContent.isLast()) {
            // if not
            // swallow content
            ((HttpContent) ctx.getMessage()).getContent().toStringContent();
            return ctx.getStopAction();
        }

        // if entire request was parsed
        // extract requested resource URL path
        final String uri = request.getRequestURI();

        logger.info("Request uri: "+ uri);
        ctx.suspend();
        final NextAction suspendAction = ctx.getSuspendAction();

        // Start asynchronous file download

        // return suspend action
        return suspendAction;
    }

    private static HttpPacket create404(HttpRequestPacket request)
            throws CharConversionException {
        // Build 404 HttpResponsePacket message headers
        final HttpResponsePacket responseHeader = HttpResponsePacket.builder(request).
                protocol(request.getProtocol()).status(404).
                reasonPhrase("Not Found").build();

        // Build 404 HttpContent on base of HttpResponsePacket message header
        return responseHeader.httpContentBuilder().
                content(Buffers.wrap(null,
                        "Can not find file, corresponding to URI: "
                                + request.getRequestURIRef().getDecodedURI())).
                build();
    }

}
