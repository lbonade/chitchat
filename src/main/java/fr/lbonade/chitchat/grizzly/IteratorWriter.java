package fr.lbonade.chitchat.grizzly;

import org.glassfish.grizzly.WriteHandler;
import org.glassfish.grizzly.http.io.NIOOutputStream;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Created by lbonade on 20/07/2014.
 */
public class IteratorWriter<T> implements WriteHandler{
    boolean first = true;

    private final Iterator<T> iterator;
    private final NIOOutputStream out;
    private final Response response;
    private final Function<T,String> function;

    IteratorWriter( final Iterator<T> iterator, final NIOOutputStream out, final Response response, final Function<T,String> function) {
           this.function = function;
        this.iterator = iterator;
        this.out = out;
        this.response = response;
    }

    @Override
    public void onWritePossible() throws Exception {
        if (iterator.hasNext()) {

            String doc = function.apply(iterator.next());
            String value = (first ? "[" : "") + doc + (iterator.hasNext() ? ",\n" : "]");
            first = false;
            out.write(value.getBytes("UTF-8"));
            if (iterator.hasNext()) {
                out.notifyCanWrite(this);
            } else {
                out.close();
                response.setStatus(HttpStatus.OK_200);
                response.flush();
                if (response.isSuspended()) {
                    response.resume();
                } else {
                    response.finish();
                }
            }
        } else {
            out.close();
            response.setStatus(HttpStatus.OK_200);
            response.flush();
            if (response.isSuspended()) {
                response.resume();
            } else {
                response.finish();
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
            out.close();
            if (response.isSuspended()) {
                response.resume();
            } else {
                response.finish();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
