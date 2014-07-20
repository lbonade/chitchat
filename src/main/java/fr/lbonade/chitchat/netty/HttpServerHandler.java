package fr.lbonade.chitchat.netty;


import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import fr.lbonade.chitchat.CouchBaseClientFactory;
import fr.lbonade.chitchat.TraitementType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import jdk.nashorn.internal.ir.*;
import jdk.nashorn.internal.parser.JSONParser;
import jdk.nashorn.internal.runtime.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
/**
 * Created by lbonade on 19/07/2014.
 */
public class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServerHandler.class);
    private final Query queryLatest;
    private final View viewLatest;
    private final Query queryThread;
    private final View viewThread;
    private final Query queryTextReversed;
    private final View viewTextReversed;
    private final Query queryTextDirect;
    private final View viewTextDrect;

    private final MessageDigest cript;
    private final CouchbaseClient cbc;
    private String uri;
    private HttpRequest request;
    private TraitementType type = TraitementType.BAD_REQUEST;
    /** Buffer that stores the response content */
    private final StringBuilder buf = new StringBuilder();

    private final EventExecutorGroup executor;
    public HttpServerHandler(EventExecutorGroup executor) throws NoSuchAlgorithmException, URISyntaxException, IOException{
        try {
            cript = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("SHA-1 not found", e);
            throw e;
        }
        cbc = CouchBaseClientFactory.getInstance();
        queryLatest = new Query();
        queryLatest.setIncludeDocs(false);
        queryLatest.setGroup(true);
        queryLatest.setGroupLevel(1);
        viewLatest = cbc.getView("documents", "latest");
        queryThread = new Query();
        queryThread.setIncludeDocs(true);
        queryThread.setInclusiveEnd(true);
        viewThread = cbc.getView("documents", "threads");

        queryTextReversed = new Query();
        queryTextReversed.setIncludeDocs(false);
        queryTextReversed.setInclusiveEnd(false);
        viewTextReversed = cbc.getView("documents", "search_inverse");
        queryTextDirect = new Query();
        queryTextDirect.setIncludeDocs(true);
        queryTextDirect.setInclusiveEnd(false);
        viewTextDrect = cbc.getView("documents", "search_direct");
        this.executor = executor;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest request = this.request = (HttpRequest) msg;
            boolean keepAlive = isKeepAlive(request);
            if (is100ContinueExpected(request)) {
                send100Continue(ctx);
            }

            uri = request.getUri();
            buf.setLength(0);
            if (uri.equals("/chitchat")) {

                type = TraitementType.WRITE;
            } else if (uri.startsWith("/chitchat/latest/")) {
                type = TraitementType.LATEST;
                QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
                Map<String, List<String>> params = queryStringDecoder.parameters();
                if (!params.isEmpty()) {
                    for (Map.Entry<String, List<String>> p : params.entrySet()) {
                        String key = p.getKey();
                        List<String> vals = p.getValue();
                        for (String val : vals) {
                            buf.append("PARAM: ").append(key).append(" = ").append(val).append("\r\n");
                        }
                    }
                    buf.append("\r\n");
                }
            } else if (uri.startsWith("/chitchat/thread/")) {
                type = TraitementType.THREAD;
            } else if (uri.startsWith("/chitchat/search")) {
                type = TraitementType.SEARCH;
            } else {
                type = TraitementType.BAD_REQUEST;
            }


        }

        if (msg instanceof HttpContent) {
            HttpContent httpContent = (HttpContent) msg;

            ByteBuf content = httpContent.content();
            if (content.isReadable()) {
                buf.append(content.toString(CharsetUtil.UTF_8));
            }

            if (msg instanceof LastHttpContent) {

                LastHttpContent trailer = (LastHttpContent) msg;

                Callable<FullHttpResponse> callable = new Callable<FullHttpResponse>() {
                    @Override
                    public FullHttpResponse call() throws Exception {
                        return processResponse(ctx);
                    }
                };

                final Future<FullHttpResponse> future = executor.submit(callable);

                future.addListener(new GenericFutureListener<Future<FullHttpResponse>>() {
                    @Override
                    public void operationComplete(Future<FullHttpResponse> future)
                            throws Exception {
                        if (future.isSuccess()) {
                            FullHttpResponse response = future.get();
                            response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
                            // Decide whether to close the connection or not.
                            boolean keepAlive = isKeepAlive(request);

                            if (keepAlive) {
                                // Add 'Content-Length' header only for a keep-alive connection.
                                response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
                                // Add keep alive header as per:
                                // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                                response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                            }


                            // Write the response.
                            ctx.writeAndFlush(response);

                            if (!keepAlive) {
                                // If keep-alive is off, close the connection once the content is fully written.
                                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                            }
                        } else {
                            ctx.fireExceptionCaught(future.cause());
                        }
                    }
                });

            }
        }
    }

    private FullHttpResponse processResponse(ChannelHandlerContext ctx) {

        FullHttpResponse response;
        switch (type) {
            case WRITE:
                response = processPut();
                break;
            case LATEST :
                response = processLatest();
                break;
            case THREAD:
                response = processThread();
                break;
            case SEARCH:
                response = processSearch();
                break;
            default:
                buf.setLength(0);
                buf.append("bad request");
                response = new DefaultFullHttpResponse(
                        HTTP_1_1, BAD_REQUEST,
                        Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
                break;
        }
        return  response;
    }

    private FullHttpResponse processSearch() {
        FullHttpResponse response;QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        List<String> searchParam = params.get("q");
        if (searchParam == null || searchParam.size() != 1) {
            response = new DefaultFullHttpResponse(
                    HTTP_1_1, BAD_REQUEST,
                    Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
            return response;

        } else {
            LOG.info("search : "+uri+": parameter :"+searchParam.get(0));
            String p = searchParam.get(0);
            int indexOf = p.indexOf("*");
            if(indexOf>=0) {

                if (p.split("\\*").length > 2) {
                    buf.append("too many wildcards");
                    response = new DefaultFullHttpResponse(
                            HTTP_1_1, BAD_REQUEST,
                            Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
                    return response;
                }
                if (indexOf == 0) {
                    char[] search = p.toCharArray();
                    StringBuffer sbf = new StringBuffer();
                    for (int i = search.length-1; i>0; i--) {
                        sbf.append(search[i]);
                    }
                    LOG.info("research reversed with : "+sbf.toString());
                    ComplexKey startKey = ComplexKey.of(sbf.toString());
                    ComplexKey endKey = ComplexKey.of(sbf.toString()+"z");
                    queryTextReversed.setRangeStart(startKey);
                    queryTextReversed.setRangeEnd(endKey);
                    queryTextReversed.setInclusiveEnd(false);
                    ViewResponse viewResponse = cbc.query(viewTextReversed, queryTextReversed);
                    buf.setLength(0);
                    Set<String> ids = new HashSet<String>();
                    for (ViewRow r :viewResponse) {
                        ids.add(r.getId());
                    }
                    buf.append("[");
                    for (Iterator<Object> it = cbc.getBulk(ids).values().iterator(); it.hasNext(); ) {
                        Object obj = it.next();
                        buf.append((String)obj).append(it.hasNext()?", \n" : "");
                    }
                    buf.append("]");

                } else if (indexOf == p.length()-1) {
                    String search = p.substring(0, p.length()-2);
                    LOG.info("research form start : "+search);
                    ComplexKey startKey = ComplexKey.of(search);
                    ComplexKey endKey = ComplexKey.of(search+"z");
                    queryTextDirect.setRangeStart(startKey);
                    queryTextDirect.setRangeEnd(endKey);
                    queryTextDirect.setInclusiveEnd(false);
                    ViewResponse viewResponse = cbc.query(viewTextDrect, queryTextDirect);
                    buf.setLength(0);
                    Set<String> ids = new HashSet<String>();
                    for (ViewRow r :viewResponse) {
                        ids.add(r.getId());
                    }
                    buf.append("[");
                    for (Iterator<Object> it = cbc.getBulk(ids).values().iterator(); it.hasNext(); ) {
                        Object obj = it.next();
                        buf.append((String)obj).append(it.hasNext()?", \n" : "");
                    }
                    buf.append("]");
                } else {
                    String start = p.substring(0, indexOf);
                    String end = p.substring(indexOf+1);
                    char[] chars = end.toCharArray();
                    StringBuffer sbf = new StringBuffer(end.length());
                    for (int i = chars.length-1; i>0; i--) {
                        sbf.append(chars[i]);
                    }
                    end = sbf.toString();
                    final ComplexKey directStartKey = ComplexKey.of(start);
                    final ComplexKey directEndKey = ComplexKey.of(start+"z");
                    final ComplexKey reverseStartKey = ComplexKey.of(end);
                    final ComplexKey reverseEndKey = ComplexKey.of(end+"z");

                    Callable<Set<String>> direct = new Callable<Set<String>>() {
                        @Override
                        public Set<String> call() throws Exception {
                            queryTextDirect.setRangeStart(directStartKey);
                            queryTextDirect.setRangeEnd(directEndKey);
                            queryTextDirect.setInclusiveEnd(true);
                            ViewResponse viewResponse = cbc.query(viewTextDrect, queryTextDirect);
                            Set<String> ids = new HashSet<String>();
                            for (ViewRow r :viewResponse) {
                                ids.add(r.getId());
                            }
                            return ids;
                        }
                    };
                    Callable<Set<String>> reverse = new Callable<Set<String>>() {
                        @Override
                        public Set<String> call() throws Exception {
                            queryTextReversed.setRangeStart(reverseStartKey);
                            queryTextReversed.setRangeEnd(reverseEndKey);
                            queryTextReversed.setInclusiveEnd(true);
                            ViewResponse viewResponse = cbc.query(viewTextReversed, queryTextReversed);
                            Set<String> ids = new HashSet<String>();
                            for (ViewRow r :viewResponse) {
                                ids.add(r.getId());
                            }
                            return ids;
                        }
                    };
                    final Set<String> ids = new HashSet<String>();
                    final CountDownLatch startSignal = new CountDownLatch(2);
                    GenericFutureListener<Future<Set<String>>> l = new GenericFutureListener<Future<Set<String>>>() {
                        int count = 0;
                        private final String lock = "";
                        @Override
                        public void operationComplete(Future<Set<String>> future) throws Exception {
                            synchronized (lock) {
                                Set<String> r = future.get();
                                if (count == 0) {
                                    ids.addAll(r);
                                } else {
                                    ids.retainAll(r);
                                }
                                startSignal.countDown();
                            }
                        }
                    };
                        Future<Set<String>> future1 = executor.submit(direct);
                        Future<Set<String>> future2 = executor.submit(reverse);
                    future1.addListener(l);
                    future2.addListener(l);
                    try {
                        startSignal.await();
                    } catch (InterruptedException ie) {
                        LOG.warn("interrupted waiting", ie);
                    }
                        buf.setLength(0);
                        buf.append("[");
                        for (Iterator<Object> it = cbc.getBulk(ids).values().iterator(); it.hasNext(); ) {
                            Object obj = it.next();
                            buf.append((String)obj).append(it.hasNext()?", \n" : "");
                        }
                        buf.append("]");
                    LOG.info("research with start "+ start+ " and end" + end);
                }
            } else {
                ComplexKey key = ComplexKey.of(p);
                queryTextDirect.setRangeStart(key);
                queryTextDirect.setRangeEnd(key);
                queryTextDirect.setInclusiveEnd(true);
                ViewResponse viewResponse = cbc.query(viewTextDrect, queryTextDirect);
                buf.setLength(0);
                Set<String> ids = new HashSet<String>();
                for (ViewRow r :viewResponse) {
                    ids.add(r.getId());
                }
                buf.append("[");
                for (Iterator<Object> it = cbc.getBulk(ids).values().iterator(); it.hasNext(); ) {
                    Object obj = it.next();
                    buf.append((String)obj).append(it.hasNext()?", \n" : "");
                }
                buf.append("]");
            }

            response = new DefaultFullHttpResponse(
                    HTTP_1_1, OK,
                    Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
        }
        return response;
    }

    private FullHttpResponse processThread() {
        FullHttpResponse response;
        String thread = uri.substring("/chitchat/thread/".length());
        LOG.info("thread : "+uri+": parameter :"+thread);
        ComplexKey key = ComplexKey.of(Long.parseLong(thread));
        queryThread.setIncludeDocs(true);
        queryThread.setRangeStart(key);
        queryThread.setRangeEnd(key);
        ViewResponse viewResponse = cbc.query(viewThread, queryThread);
        buf.setLength(0);
        for (ViewRow r :viewResponse) {
            buf.append(r.getDocument());
        }
        response = new DefaultFullHttpResponse(
                HTTP_1_1, OK,
                Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
        return response;
    }

    private FullHttpResponse processLatest() {
        FullHttpResponse response;
        String author = uri.substring("/chitchat/latest/".length());
        LOG.info("latest : "+uri+": parameter :"+author);

        ComplexKey key = ComplexKey.of(author);
        queryLatest.setRangeStart(key);
        queryLatest.setRangeEnd(key);
        ViewResponse viewResponse = cbc.query(viewLatest, queryLatest);
        buf.setLength(0);
        for (ViewRow r :viewResponse) {
            buf.append(r.getValue());
        }
        response = new DefaultFullHttpResponse(
                HTTP_1_1, OK,
                Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
        return response;
    }

    private FullHttpResponse processPut() {
        FullHttpResponse response;
        String doc = buf.toString();
        Source src = new Source("message", doc.toCharArray());
        JSONParser parser = new JSONParser(src, null);
        ObjectNode node = (ObjectNode)parser.parse();
        String author = "";
        for (PropertyNode pn : node.getElements()) {
            if (pn.getKeyName().equals("author")) {
                author = pn.getValue().toString();
                break;
            }
        }
        doc = doc.substring(0, doc.lastIndexOf("}"))+", \"date\":"+System.nanoTime()+"}";
        cript.reset();
        try {
            cript.update(doc.getBytes("utf8"));
        } catch (UnsupportedEncodingException e) {
            LOG.error("Encoding utf-8 non trouvé", e);
            cript.update(doc.getBytes());
        }
        String sha1 = new String(Base64.getEncoder().encode(cript.digest()));
        String idDoc ="document-"+sha1;
        cbc.set(idDoc, doc);
        LOG.info("write : " + author + " : id : "+idDoc+ "::" + buf.toString());
        buf.setLength(0);
        response = new DefaultFullHttpResponse(
                HTTP_1_1, CREATED,
                Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
        return response;
    }

    private static void send100Continue(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE);
        ctx.write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
