import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.DgraphProto.Response;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.util.Collections;
import java.util.Map;

public class App {
    private static final String TEST_HOSTNAME = "localhost";
    private static final int TEST_PORT = 9080;

    private static DgraphClient createDgraphClient(boolean withAuthHeader) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext().build();
        DgraphStub stub = DgraphGrpc.newStub(channel);
        if (withAuthHeader) {
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("auth-token", Metadata.ASCII_STRING_MARSHALLER), "the-auth-token-value");
            stub = MetadataUtils.attachHeaders(stub, metadata);
        }
        return new DgraphClient(stub);
    }

    public static void main(final String[] args) {
        // 创建连接
        DgraphClient dgraphClient = createDgraphClient(false);
        dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());

        // 创建schema
        String schema = "name: string @index(exact) .";
        Operation operation = Operation.newBuilder().setSchema(schema).build();
        dgraphClient.alter(operation);

        // 增加数据
        Gson gson = new Gson();
        // 开启事务
        Transaction txn = dgraphClient.newTransaction();
        try {
            Person p = new Person();
            p.name = "Alice";
            // Serialize it
            String json = gson.toJson(p);
            // Run mutation
            Mutation mutation = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json)).build();
            txn.mutate(mutation);
            txn.commit();
        } finally {
            txn.discard();
        }

        // 查询
        String query =
                "query all($a: string){" +
                        " all(func: eq(name, $a)) {" +
                        "  name" +
                        "  }" +
                        "}";
        Map<String, String> vars = Collections.singletonMap("$a", "Alice");
        Response res = dgraphClient.newTransaction().queryWithVars(query, vars);
        System.out.println(res.getJson().toStringUtf8());
        People ppl = gson.fromJson(res.getJson().toStringUtf8(), People.class);
        System.out.printf("people found: %d\n", ppl.all.size());
        ppl.all.forEach(person -> System.out.println(person.name));
    }

}
