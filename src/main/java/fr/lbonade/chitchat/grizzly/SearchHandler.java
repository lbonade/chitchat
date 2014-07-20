package fr.lbonade.chitchat.grizzly;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import fr.lbonade.chitchat.CouchBaseClientFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
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
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Created by lbonade on 20/07/2014.
 */
public class SearchHandler  extends HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(SearchHandler.class);
    private final EventExecutorGroup executor;

    public SearchHandler(EventExecutorGroup executor) {
        this.executor = executor;
    }

    @Override
    public void service(final Request request, final Response response) throws Exception {
        response.setBufferSize(0);
        if(request.getMethod() != Method.GET) {
            response.setStatus(HttpStatus.BAD_REQUEST_400.getStatusCode(), "Get required");
            return;
        }

        String params = request.getParameter("q");
        if (params == null || params.length() == 0) {
            response.setStatus(HttpStatus.BAD_REQUEST_400.getStatusCode(), "Mauvaise requete");
            return;
        }
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
        final NIOOutputStream out = response.getNIOOutputStream();
        response.suspend();

        int indexOf = params.indexOf("*");
        final CouchbaseClient cbc = CouchBaseClientFactory.getInstance();
        final Query queryTextReversed = new Query();
        final View viewTextReversed = cbc.getView("documents", "search_inverse");
        final Query queryTextDirect = new Query();
        final View viewTextDrect = cbc.getView("documents", "search_direct");
        queryTextReversed.setIncludeDocs(false);
        queryTextReversed.setInclusiveEnd(false);
        queryTextDirect.setIncludeDocs(true);
        queryTextDirect.setInclusiveEnd(false);

        if(indexOf>=0) {
            if (params.split("\\*").length > 2) {
                response.setStatus(404, "Too Many WildCards");
                response.resume();
                return;
            }
            if (indexOf == 0) {
                char[] search = params.toCharArray();
                StringBuffer sbf = new StringBuffer();
                for (int i = search.length-1; i>0; i--) {
                    sbf.append(search[i]);
                }
                logger.info("research reversed with : "+sbf.toString());
                ComplexKey startKey = ComplexKey.of(sbf.toString());
                ComplexKey endKey = ComplexKey.of(sbf.toString()+"z");
                queryTextReversed.setRangeStart(startKey);
                queryTextReversed.setRangeEnd(endKey);
                queryTextReversed.setInclusiveEnd(false);
                ViewResponse viewResponse = cbc.query(viewTextReversed, queryTextReversed);
                Set<String> ids = new HashSet<String>();
                for (ViewRow r :viewResponse) {
                    ids.add(r.getId());
                }
                Iterator<Object> it = cbc.getBulk(ids).values().iterator();
                out.notifyCanWrite(new IteratorWriter<Object>(it, out, response, (r) -> {
                    return (String) r;
                }));
            } else if (indexOf == params.length()-1) {
                String search = params.substring(0, params.length()-2);
                logger.info("research form start : "+search);
                ComplexKey startKey = ComplexKey.of(search);
                ComplexKey endKey = ComplexKey.of(search+"z");
                queryTextDirect.setRangeStart(startKey);
                queryTextDirect.setRangeEnd(endKey);
                queryTextDirect.setInclusiveEnd(false);
                ViewResponse viewResponse = cbc.query(viewTextDrect, queryTextDirect);
                Set<String> ids = new HashSet<String>();
                for (ViewRow r :viewResponse) {
                    ids.add(r.getId());
                }
                Iterator<Object> it = cbc.getBulk(ids).values().iterator();
                out.notifyCanWrite(new IteratorWriter<Object>(it, out, response, (r) -> {
                    return (String) r;
                }));
            } else {
                String start = params.substring(0, indexOf);
                String end = params.substring(indexOf+1);
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
                logger.info("research with start "+ start+ " and end" + end);

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
                            count++;
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
                    logger.warn("interrupted waiting", ie);
                }
                Iterator<Object> it = cbc.getBulk(ids).values().iterator();
                out.notifyCanWrite(new IteratorWriter<Object>(it, out, response, (r) -> {
                    return (String) r;
                }));
            }

        } else {
            ComplexKey key = ComplexKey.of(params);
            queryTextDirect.setRangeStart(key);
            queryTextDirect.setRangeEnd(key);
            queryTextDirect.setInclusiveEnd(true);
            Set<String> ids = new HashSet<String>();
            ViewResponse viewResponse = cbc.query(viewTextDrect, queryTextDirect);
            for (ViewRow r :viewResponse) {
                ids.add(r.getId());
            }
            out.notifyCanWrite(new IteratorWriter<Object>(cbc.getBulk(ids).values().iterator(), out, response, (r) -> {
                return (String) r;
            }));

        }

    }

}
