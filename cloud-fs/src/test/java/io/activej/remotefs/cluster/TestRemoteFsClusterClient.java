package io.activej.remotefs.cluster;

import io.activej.async.function.AsyncConsumer;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.StacklessException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.net.AbstractServer;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.remotefs.FileMetadata;
import io.activej.remotefs.FsClient;
import io.activej.remotefs.RemoteFsClient;
import io.activej.remotefs.RemoteFsServer;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.activej.common.collection.CollectionUtils.concat;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public final class TestRemoteFsClusterClient {
	public static final int CLIENT_SERVER_PAIRS = 10;
	public static final int REPLICATION_COUNT = 4;

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final List<Path> serverStorages = new ArrayList<>();

	private List<RemoteFsServer> servers;
	private Path clientStorage;
	private FsPartitions partitions;
	private RemoteFsClusterClient client;

	@Before
	public void setup() throws IOException {
		Executor executor = Executors.newSingleThreadExecutor();
		servers = new ArrayList<>(CLIENT_SERVER_PAIRS);
		clientStorage = Paths.get(tmpFolder.newFolder("client").toURI());

		Files.createDirectories(clientStorage);

		Map<Object, FsClient> clients = new HashMap<>(CLIENT_SERVER_PAIRS);

		Eventloop eventloop = Eventloop.getCurrentEventloop();

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			InetSocketAddress address = new InetSocketAddress("localhost", 5600 + i);

			clients.put("server_" + i, RemoteFsClient.create(eventloop, address));

			Path path = Paths.get(tmpFolder.newFolder("storage_" + i).toURI());
			serverStorages.add(path);
			Files.createDirectories(path);

			RemoteFsServer server = RemoteFsServer.create(eventloop, executor, path).withListenAddress(address);
			server.listen();
			servers.add(server);
		}

		clients.put("dead_one", RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5555)));
		clients.put("dead_two", RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5556)));
		clients.put("dead_three", RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5557)));

		partitions = FsPartitions.create(eventloop, clients);
		client = RemoteFsClusterClient.create(partitions);
		client.withReplicationCount(REPLICATION_COUNT); // there are those 3 dead nodes added above
	}

	@Test
	public void testUpload() throws IOException {
		String content = "test content of the file";
		String resultFile = "file.txt";

		await(client.upload(resultFile)
				.then(ChannelSupplier.of(ByteBuf.wrapForReading(content.getBytes(UTF_8)))::streamTo)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		int uploaded = 0;
		for (Path path : serverStorages) {
			Path resultPath = path.resolve(resultFile);
			if (Files.exists(resultPath)) {
				assertEquals(new String(readAllBytes(resultPath), UTF_8), content);
				uploaded++;
			}
		}
		assertEquals(4, uploaded); // replication count

	}

	@Test
	public void testDownload() throws IOException {
		int numOfServer = 3;
		String file = "the_file.txt";
		String content = "another test content of the file";

		Files.write(serverStorages.get(numOfServer).resolve(file), content.getBytes(UTF_8));

		await(ChannelSupplier.ofPromise(client.download(file))
				.streamTo(ChannelFileWriter.open(newCachedThreadPool(), clientStorage.resolve(file)))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertEquals(new String(readAllBytes(clientStorage.resolve(file)), UTF_8), content);
	}

	@Test
	public void testUploadSelector() throws IOException {
		String content = "test content of the file";
		ByteBuf data = ByteBuf.wrapForReading(content.getBytes(UTF_8));

		client.withReplicationCount(1);
		partitions
				.withServerSelector((fileName, serverKeys) -> {
					String firstServer = fileName.contains("1") ?
							"server_1" :
							fileName.contains("2") ?
									"server_2" :
									fileName.contains("3") ?
											"server_3" :
											"server_0";
					return serverKeys.stream()
							.map(String.class::cast)
							.sorted(Comparator.comparing(key -> key.equals(firstServer) ? -1 : 1))
							.collect(toList());
				});

		String[] files = {"file_1.txt", "file_2.txt", "file_3.txt", "other.txt"};

		await(Promises.all(Arrays.stream(files).map(f -> ChannelSupplier.of(data.slice()).streamTo(ChannelConsumer.ofPromise(client.upload(f)))))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertEquals(new String(readAllBytes(serverStorages.get(1).resolve("file_1.txt")), UTF_8), content);
		assertEquals(new String(readAllBytes(serverStorages.get(2).resolve("file_2.txt")), UTF_8), content);
		assertEquals(new String(readAllBytes(serverStorages.get(3).resolve("file_3.txt")), UTF_8), content);
		assertEquals(new String(readAllBytes(serverStorages.get(0).resolve("other.txt")), UTF_8), content);
	}

	@Test
	@Ignore("this test uses lots of local sockets (and all of them are in TIME_WAIT state after it for a minute) so HTTP tests after it may fail indefinitely")
	public void testUploadAlot() throws IOException {
		String content = "test content of the file";
		ByteBuf data = ByteBuf.wrapForReading(content.getBytes(UTF_8));

		await(Promises.sequence(IntStream.range(0, 1_000)
				.mapToObj(i ->
						() -> ChannelSupplier.of(data.slice())
								.streamTo(ChannelConsumer.ofPromise(client.upload("file_uploaded_" + i + ".txt")))))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		for (int i = 0; i < 1000; i++) {
			int replicasCount = 0;
			for (Path path : serverStorages) {
				Path targetPath = path.resolve("file_uploaded_" + i + ".txt");
				if (Files.exists(targetPath) && Arrays.equals(content.getBytes(), readAllBytes(targetPath))) {
					replicasCount++;
				}
			}
			assertEquals(client.getReplicationCount(), replicasCount);
		}
	}

	@Test
	public void testNotEnoughUploads() {
		client.withReplicationCount(partitions.getClients().size()); // max possible replication

		Throwable exception = awaitException(ChannelSupplier.of(ByteBuf.wrapForReading("whatever, blah-blah".getBytes(UTF_8))).streamTo(ChannelConsumer.ofPromise(client.upload("file_uploaded.txt")))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("Didn't connect to enough partitions"));
	}

	@Test
	public void downloadNonExisting() {
		String fileName = "i_dont_exist.txt";

		Throwable exception = awaitException(ChannelSupplier.ofPromise(client.download(fileName))
				.streamTo(ChannelConsumer.of(AsyncConsumer.of(ByteBuf::recycle)))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString(fileName));
	}

	@Test
	public void testCopy() throws IOException {
		String source = "the_file.txt";
		String target = "new_file.txt";
		String content = "test content of the file";

		List<Path> paths = new ArrayList<>(serverStorages);
		Collections.shuffle(paths);

		for (Path path : paths.subList(0, REPLICATION_COUNT)) {
			Files.write(path.resolve(source), content.getBytes(UTF_8));
		}

		ByteBuf result = await(client.copy(source, target)
				.then(() -> client.download(target).then(supplier -> supplier.toCollector(ByteBufQueue.collector())))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertEquals(content, result.asString(UTF_8));

		int copies = 0;
		for (Path path : paths) {
			Path targetPath = path.resolve(target);
			if (Files.exists(targetPath) && Arrays.equals(content.getBytes(), Files.readAllBytes(targetPath))) {
				copies++;
			}
		}

		assertEquals(copies, REPLICATION_COUNT);
	}

	@Test
	public void testCopyNotEnoughPartitions() throws IOException {
		int numOfServers = REPLICATION_COUNT - 1;
		String source = "the_file.txt";
		String target = "new_file.txt";
		String content = "test content of the file";

		List<Path> paths = new ArrayList<>(serverStorages);
		Collections.shuffle(paths);

		for (Path path : paths.subList(0, numOfServers)) {
			Files.write(path.resolve(source), content.getBytes(UTF_8));
		}

		Throwable exception = awaitException(client.copy(source, target)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("Could not copy"));


		int copies = 0;
		for (Path path : paths) {
			Path targetPath = path.resolve(target);
			if (Files.exists(targetPath) && Arrays.equals(content.getBytes(), Files.readAllBytes(targetPath))) {
				copies++;
			}
		}

		assertTrue(copies >= 3 && copies < REPLICATION_COUNT);
	}

	@Test
	public void testCopyAllSingleFile() throws IOException {
		doTestCopyAll(1);
	}

	@Test
	public void testCopyAllThreeFiles() throws IOException {
		doTestCopyAll(3);
	}

	@Test
	public void testCopyAllTenFiles() throws IOException {
		doTestCopyAll(10);
	}

	@Test
	public void testCopyAllManyFiles() throws IOException {
		doTestCopyAll(100);
	}

	@Test
	public void testCopyAllNotEnoughPartitions() throws IOException {
		int numberOfServers = REPLICATION_COUNT - 1;
		Map<String, String> sourceToTarget = IntStream.range(0, 10).boxed()
				.collect(toMap(i -> "the_file_" + i + ".txt", i -> "the_new_file_" + i + ".txt"));
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String source : sourceToTarget.keySet()) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + source;
			for (Path path : paths.subList(0, numberOfServers)) {
				Files.write(path.resolve(source), content.getBytes(UTF_8));
			}
		}

		Throwable exception = awaitException(client.copyAll(sourceToTarget)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));
		assertTrue(exception.getMessage().matches("^Could not copy files \\{.*} on enough partitions.*"));
	}

	@Test
	public void testCopyAllWithMissingFiles() throws IOException {
		Map<String, String> sourceToTarget = IntStream.range(0, 10).boxed()
				.collect(toMap(i -> "the_file_" + i + ".txt", i -> "the_new_file_" + i + ".txt"));
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String source : sourceToTarget.keySet()) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + source;
			for (Path path : paths.subList(0, REPLICATION_COUNT)) {
				Files.write(path.resolve(source), content.getBytes(UTF_8));
			}
		}

		// adding non-existent file to mapping
		sourceToTarget.put("nonexistent.txt", "new_nonexistent.txt");

		Throwable exception = awaitException(client.copyAll(sourceToTarget)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));
		assertTrue(exception.getMessage().startsWith("Could not find all files"));
	}

	@Test
	public void testMoveAllSingleFile() throws IOException {
		doTestMoveAll(1);
	}

	@Test
	public void testMoveAllThreeFiles() throws IOException {
		doTestMoveAll(3);
	}

	@Test
	public void testMoveAllTenFiles() throws IOException {
		doTestMoveAll(10);
	}

	@Test
	public void testMoveAllManyFiles() throws IOException {
		doTestMoveAll(100);
	}

	@Test
	public void testMoveAllNotEnoughPartitions() throws IOException {
		int numberOfServers = REPLICATION_COUNT - 1;
		Map<String, String> sourceToTarget = IntStream.range(0, 10).boxed()
				.collect(toMap(i -> "the_file_" + i + ".txt", i -> "the_new_file_" + i + ".txt"));
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String source : sourceToTarget.keySet()) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + source;
			for (Path path : paths.subList(0, numberOfServers)) {
				Files.write(path.resolve(source), content.getBytes(UTF_8));
			}
		}

		Throwable exception = awaitException(client.copyAll(sourceToTarget)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertTrue(exception.getMessage().matches("^Could not copy files \\{.*} on enough partitions.*"));
	}

	@Test
	public void testMoveAllWithMissingFiles() throws IOException {
		Map<String, String> sourceToTarget = IntStream.range(0, 10).boxed()
				.collect(toMap(i -> "the_file_" + i + ".txt", i -> "the_new_file_" + i + ".txt"));
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String source : sourceToTarget.keySet()) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + source;
			for (Path path : paths.subList(0, REPLICATION_COUNT)) {
				Files.write(path.resolve(source), content.getBytes(UTF_8));
			}
		}

		// adding non-existent file to mapping
		sourceToTarget.put("nonexistent.txt", "new_nonexistent.txt");

		Throwable exception = awaitException(client.copyAll(sourceToTarget)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));
		assertTrue(exception.getMessage().startsWith("Could not find all files:"));
	}

	@Test
	public void testInspectAll() throws IOException {
		List<String> names = IntStream.range(0, 10)
				.mapToObj(i -> "the_file_" + i + ".txt")
				.collect(toList());
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String name : names) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + name;
			for (Path path : paths.subList(0, ThreadLocalRandom.current().nextInt(3) + 1)) {
				Files.write(path.resolve(name), content.getBytes(UTF_8));
			}
		}

		Map<String, @Nullable FileMetadata> result = await(client.infoAll(names)
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertEquals(names.size(), result.size());
		for (String name : names) {
			FileMetadata metadata = result.get(name);
			assertNotNull(metadata);
			assertEquals(name, metadata.getName());
		}
	}

	@Test
	public void testInspectAllWithMissingFiles() throws IOException {
		List<String> existingNames = IntStream.range(0, 10)
				.mapToObj(i -> "the_file_" + i + ".txt")
				.collect(toList());
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String name : existingNames) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + name;
			for (Path path : paths.subList(0, ThreadLocalRandom.current().nextInt(3) + 1)) {
				Files.write(path.resolve(name), content.getBytes(UTF_8));
			}
		}

		List<String> nonExistingNames = IntStream.range(0, 10)
				.mapToObj(i -> "nonexistent_" + i + ".txt")
				.collect(toList());

		Map<String, @Nullable FileMetadata> result = await(client.infoAll(concat(existingNames, nonExistingNames))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		assertEquals(existingNames.size() + nonExistingNames.size(), result.size());
		for (String name : existingNames) {
			FileMetadata metadata = result.get(name);
			assertNotNull(metadata);
			assertEquals(name, metadata.getName());
		}

		for (String name : nonExistingNames) {
			assertTrue(result.containsKey(name));
			assertNull(result.get(name));
		}
	}

	private void doTestCopyAll(int numberOfFiles) throws IOException {
		Map<String, String> sourceToTarget = IntStream.range(0, numberOfFiles).boxed()
				.collect(toMap(i -> "the_file_" + i + ".txt", i -> "the_new_file_" + i + ".txt"));
		doActionAndAssertFilesAreCopied(sourceToTarget, client::copyAll);
	}

	private void doTestMoveAll(int numberOfFiles) throws IOException {
		Map<String, String> sourceToTarget = IntStream.range(0, numberOfFiles).boxed()
				.collect(toMap(i -> "the_file_" + i + ".txt", i -> "the_new_file_" + i + ".txt"));
		doActionAndAssertFilesAreCopied(sourceToTarget, client::moveAll);

		for (Path serverStorage : serverStorages) {
			for (String s : sourceToTarget.keySet()) {
				if (Files.exists(serverStorage.resolve(s))) {
					fail();
				}
			}
		}
	}

	private void doActionAndAssertFilesAreCopied(Map<String, String> sourceToTarget, Function<Map<String, String>, Promise<Void>> action) throws IOException {
		String contentPrefix = "test content of the file ";
		List<Path> paths = new ArrayList<>(serverStorages);

		for (String source : sourceToTarget.keySet()) {
			Collections.shuffle(paths); // writing each source to random partitions

			String content = contentPrefix + source;
			for (Path path : paths.subList(0, REPLICATION_COUNT)) {
				Files.write(path.resolve(source), content.getBytes(UTF_8));
			}
		}

		List<String> results = await(action.apply(sourceToTarget)
				.then(() -> Promises.toList(sourceToTarget.values().stream()
						.map(target -> client.download(target)
								.then(supplier -> supplier.toCollector(ByteBufQueue.collector()))
								.map(byteBuf -> byteBuf.asString(UTF_8)))))
				.whenComplete(() -> servers.forEach(AbstractServer::close)));

		Set<String> expectedContents = sourceToTarget.keySet().stream().map(source -> contentPrefix + source).collect(toSet());

		for (String result : results) {
			assertTrue(expectedContents.contains(result));
			expectedContents.remove(result);
		}
		assertTrue(expectedContents.isEmpty());

		Map<String, String> expected = sourceToTarget.entrySet().stream()
				.collect(toMap(Map.Entry::getValue, entry -> contentPrefix + entry.getKey()));

		for (Map.Entry<String, String> entry : expected.entrySet()) {
			int copies = 0;
			for (Path path : paths) {
				Path targetPath = path.resolve(entry.getKey());
				if (Files.exists(targetPath) && Arrays.equals(entry.getValue().getBytes(), Files.readAllBytes(targetPath))) {
					copies++;
				}
			}
			assertEquals(copies, REPLICATION_COUNT);
		}
	}


}
