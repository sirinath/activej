import io.activej.csp.file.ChannelFileReader;
import io.activej.di.Injector;
import io.activej.di.annotation.Inject;
import io.activej.di.annotation.Provides;
import io.activej.di.module.Module;
import io.activej.eventloop.Eventloop;
import io.activej.launcher.Launcher;
import io.activej.remotefs.RemoteFsClient;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * This example demonstrates uploading file to server using RemoteFS
 * To run this example you should first launch ServerSetupExample
 */
@SuppressWarnings("unused")
public final class FileUploadExample extends Launcher {
	private static final int SERVER_PORT = 6732;
	private static final String FILE_NAME = "example.txt";

	private Path clientFile;

	@Override
	protected void onInit(Injector injector) throws Exception {
		clientFile = Files.createTempFile("example", ".txt");
		Files.write(clientFile, "example text".getBytes());
	}

	@Inject
	private RemoteFsClient client;

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RemoteFsClient remoteFsClient(Eventloop eventloop) {
		return RemoteFsClient.create(eventloop, new InetSocketAddress(SERVER_PORT));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	//[START EXAMPLE]
	@Override
	protected void run() throws Exception {
		ExecutorService executor = newSingleThreadExecutor();
		CompletableFuture<Void> future = eventloop.submit(() ->
				// consumer result here is a marker of it being successfully uploaded
				ChannelFileReader.open(executor, clientFile)
						.then(cfr -> cfr.streamTo(client.upload(FILE_NAME)))
						.whenResult(() -> System.out.printf("%nFile '%s' successfully uploaded%n%n", FILE_NAME))
		);
		future.get();
		executor.shutdown();
	}
	//[END EXAMPLE]

	public static void main(String[] args) throws Exception {
		FileUploadExample example = new FileUploadExample();
		example.launch(args);
	}
}