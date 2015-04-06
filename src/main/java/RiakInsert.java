import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.netty.PingHealthCheck;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class RiakInsert {

    private static RiakClient client;

    public static void main(String[] args) throws Exception {
        client = client();

        while(true) {
            String result = insertData();
        }
    }

    private static String insertData() throws ExecutionException, InterruptedException {

        Namespace ns = new Namespace("default", "token");

        String id = generateUUID();
        String value = generateUUID();

        Location location = new Location(ns, id);

        RiakObject riakObject = new RiakObject();
        riakObject.setContentType("text/plain");
        riakObject.setValue(BinaryValue.create(value));
        StoreValue store = new StoreValue.Builder(riakObject)
                .withLocation(location)
                .withOption(StoreValue.Option.W, new Quorum(3))
                .build();

        client.execute(store);

        System.out.println(id + "=" + value);

        return id;
    }

    // This will create a client object that we can use to interact with Riak
    private static RiakCluster setUpCluster() throws UnknownHostException {
        RiakNode node1 = new RiakNode.Builder()
                .withRemoteAddress("127.0.0.1")
                .withRemotePort(2001)
                .withHealthCheck(new PingHealthCheck())
                .build();

        RiakNode node2 = new RiakNode.Builder()
                .withRemoteAddress("127.0.0.1")
                .withRemotePort(2002)
                .withHealthCheck(new PingHealthCheck())
                .build();

        RiakNode node3 = new RiakNode.Builder()
                .withRemoteAddress("127.0.0.1")
                .withRemotePort(2003)
                .withHealthCheck(new PingHealthCheck())
                .build();

        // This cluster object takes our one node as an argument
        RiakCluster cluster = new RiakCluster.Builder(new ArrayList<RiakNode>() {
            {
                add(node1);
                add(node2);
                add(node3);
            }
        })
                .build();

        // The cluster must be started to work, otherwise you will see errors
        cluster.start();

        return cluster;
    }

    private static RiakClient client() throws UnknownHostException {
        RiakClient client = new RiakClient(setUpCluster());

        return client;
    }

    private static String generateUUID() {
        return UUID.randomUUID().toString();
    }


}
