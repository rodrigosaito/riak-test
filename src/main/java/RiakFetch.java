import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.netty.PingHealthCheck;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class RiakFetch {

    private static RiakClient client;

    public static void main(String[] args) throws Exception {
        client = client();

        try (BufferedReader br = new BufferedReader(new FileReader("keys"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] keyValue = line.split("=");

                String value = fetchData(keyValue[0]);

                if (value == null || !value.equals(keyValue[1])) {
                    System.out.println("Not Found: " + keyValue[0]);
                }
            }
        }
    }

    private static String fetchData(final String id) throws ExecutionException, InterruptedException {
        Namespace ns = new Namespace("default", "token");
        Location location = new Location(ns, id);
        FetchValue fv = new FetchValue.Builder(location).build();
        FetchValue.Response response = client.execute(fv);
        RiakObject obj = response.getValue(RiakObject.class);

        if (obj != null && obj.getValue() != null) {
            String result = obj.getValue().toString();

            System.out.println(id + "=" + result);

            return result;
        } else {
            return null;
        }
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
}
