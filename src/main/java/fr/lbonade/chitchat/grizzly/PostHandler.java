package fr.lbonade.chitchat.grizzly;

import com.couchbase.client.CouchbaseClient;
import fr.lbonade.chitchat.CouchBaseClientFactory;
import jdk.nashorn.internal.ir.ObjectNode;
import jdk.nashorn.internal.ir.PropertyNode;
import jdk.nashorn.internal.parser.JSONParser;
import jdk.nashorn.internal.runtime.Source;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.ReadHandler;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.io.NIOInputStream;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Created by lbonade on 19/07/2014.
 */
public class PostHandler extends HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(PostHandler.class);

    private final MessageDigest cript;
    private final CouchbaseClient cbc;

    public PostHandler() throws NoSuchAlgorithmException, URISyntaxException, IOException {
        try {
            cript = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            logger.error("SHA-1 not found", e);
            throw e;
        }

        cbc = CouchBaseClientFactory.getInstance();

    }

    @Override
    public void service(final Request request, final Response response) throws Exception {
        if(!request.getMethod().toString().equals(Method.POST.toString())&& !request.getMethod().toString().equals(Method.PUT.toString()) ) {
            response.setStatus(HttpStatus.BAD_REQUEST_400.getStatusCode(), "PUT or POST required");
            return;
        }

        final NIOInputStream nioInputStream = request.getNIOInputStream();
        response.suspend();
        //InputStream input = request.getInputStream();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
                    nioInputStream.notifyAvailable(new ReadHandler() {

                        @Override
                        public void onDataAvailable() throws Exception {
                            processAvailable(baos, nioInputStream);
                            nioInputStream.notifyAvailable(this);
                        }

                        @Override
                        public void onAllDataRead() throws Exception {
                            processAvailable(baos, nioInputStream);
                            complete(false);
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.warn("[onError]"+ t);
                            response.setStatus(500, t.getMessage());
                            complete(true);
                            if (response.isSuspended()) {
                                response.resume();
                            } else {
                                response.finish();
                            }
                        }

                        private void complete(boolean error) {
                            try {
                                nioInputStream.close();
                            } catch (IOException e) {
                            }
                            String message;
                            try {
                                message = new String(baos.toByteArray(), "UTF-8");
                                logger.info(message);
                            }catch (UnsupportedEncodingException ex) {
                                message = new String(baos.toByteArray());
                            }
                            if (!error) {
                                Source src = new Source("message", message.toCharArray());
                                JSONParser parser = new JSONParser(src, null);
                                ObjectNode node = (ObjectNode)parser.parse();
/*                                String author = "";
                                for (PropertyNode pn : node.getElements()) {
                                    if (pn.getKeyName().equals("author")) {
                                        author = pn.getValue().toString();
                                        break;
                                    }
                                }
*/
                                message = message.substring(0, message.lastIndexOf("}"))+", \"date\":"+System.nanoTime()+"}";
                                cript.reset();
                                byte[] bytes;
                                try {
                                    bytes = message.getBytes("utf8");
                                } catch (UnsupportedEncodingException e) {
                                    logger.error("Encoding utf-8 non trouvé", e);
                                    bytes = message.getBytes();
                                }
                                cript.update(bytes);
                                String sha1 = new String(Base64.getEncoder().encode(cript.digest()));
                                String idDoc ="document-"+sha1;
                                cbc.set(idDoc, message);
                                logger.info("write : "/* + author */+ " : id : "+idDoc+ "::" + message);
                                response.setStatus(HttpStatus.CREATED_201.getStatusCode(), "OK");
                            }
                            response.resume();
                        }
                    });
                    response.setContentType("text/plain");
                    response.getWriter().write("Simple task is done!");
    }

    private static void processAvailable(OutputStream bba, NIOInputStream in) {
        final Buffer buffer = in.readBuffer();
        // Retrieve ByteBuffer
        final ByteBuffer byteBuffer = buffer.toByteBuffer();
        try {
            while (byteBuffer.hasRemaining()) {
                // Write the ByteBuffer content to the file
                bba.write(byteBuffer.get());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // we can try to dispose the buffer
            buffer.tryDispose();
        }
    }

}
