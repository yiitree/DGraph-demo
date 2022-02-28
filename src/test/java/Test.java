import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author Jerry.Zeng
 * @date 2022/2/24
 */
public class Test {
    private static final String TEST_HOSTNAME = "localhost";
    private static final int TEST_PORT = 9080;


    public static void main(final String[] args) {


        // 创建客户端连接
        ManagedChannel channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext().build();
        DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
        DgraphClient dgraphClient = new DgraphClient(stub);

        // 删除之前数据
//        dgraphClient.alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());

        // 创建schema - 表结构
        String schema = "name: string @index(exact) .";
        DgraphProto.Operation operation = DgraphProto.Operation.newBuilder().setSchema(schema).build();
        dgraphClient.alter(operation);

        // 添加数据
        Transaction txn = dgraphClient.newTransaction();
        try {
            Person p = new Person();
            p.name = "Alice";
            String json = new Gson().toJson(p);
            DgraphProto.Mutation mutation = DgraphProto.Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json)).build();
            txn.mutate(mutation);
            txn.commit();
        } finally {
            txn.discard();
        }

        // 查询
        String query = "{\n" +
                "  all(func: eq(name, \"Alice\")) {\n" +
                "    name\n" +
                "  }\n" +
                "}\n";
        DgraphProto.Response res = dgraphClient.newTransaction().query(query);

        // 输出结果
        People ppl = new Gson().fromJson(res.getJson().toStringUtf8(), People.class);
        System.out.printf("people found: %d\n", ppl.all.size());
        ppl.all.forEach(person -> System.out.println(person.name));
    }
}
