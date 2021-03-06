package karp;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Search and replace data in kafka events.
 *
 * <pre>
 * Expects path to working directory with three folders:
 *  - {@code /path/old/} should contain text files with events (one per line)
 *  - {@code /path/ids/} should contain text files with original IDs to search (and replace)
 *
 * At runtime generates {@code /path/new/} generates new folder with:
 *  - {@code /path/new/new_events.txt} original events with provided IDs, 
 *      where IDs were replaced with new random ones, (one per line)
 *  - {@code /path/new/new_ids.txt} convenience, all new IDs from {@code /path/new/new_events.txt} (one per line)
 * 
 *  Has two modes of operation:
 *  - {@code raw} which is simply replaces ids by value, quite fast
 *  - {@code semantic} which tries to replace IDs only in {@code "event_id":"ID"} fragments, slower.
 * 
 *  This utility read whole files to memory, run with appropriate memory setting, e.g. {@code -Xmx5G}.
 * </pre>
 */
public final class Karp {

	private static final String OLD_DIR_NAME = "old";
	private static final String NEW_DIR_NAME = "new";
	private static final String IDS_DIR_NAME = "ids";
	private static final String NEW_EVENTS_FILE_NAME = "new_events.txt";
	private static final String NEW_IDS_FILE_NAME = "new_ids.txt";

	private static final String SEMANTIC_MODE = "semantic";

	private static final String EVENT_ID_STUB = "EVENT_ID";
	private static final String EVENT_ID_REX_TEMPLATE = "\"event_id\"\\s*:\\s*\"(" + EVENT_ID_STUB + ")\"";
	private static final String EVENT_ID_REX = "\"event_id\"\\s*:\\s*\"([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})\"";
	private static final Pattern EVETN_ID_PATTERN = Pattern.compile(EVENT_ID_REX);

	private Karp() {
	}

	/**
	 * <pre>
	 * Accepts to arguments:
	 *  {@code args[0]} - mandatory path to working directory
	 *  {@code args[1]} - (optional) mode specifier, {@code raw} by default
	 * </pre>
	 */
	public static void main(String[] args) {

		Path workDir = Paths.get(args[0]);
		boolean rawMode = args.length < 2 ? true : !SEMANTIC_MODE.equalsIgnoreCase(args[1]);

		Path resultDir = workDir.resolve(NEW_DIR_NAME);
		cleanDir(resultDir);
		Path resultFile = resultDir.resolve(NEW_EVENTS_FILE_NAME);
		createFile(resultFile);
		Path mappingFile = resultDir.resolve(NEW_IDS_FILE_NAME);
		createFile(mappingFile);

		List<String> ids = new LinkedList<>();
		List<String> events = new LinkedList<>();
		readDataFolder(workDir.resolve(IDS_DIR_NAME), ids);
		readDataFolder(workDir.resolve(OLD_DIR_NAME), events);

		final var replacement = replacer(rawMode);

		System.out.println(format("\nprocessing [%s old, %s ids] in %s mode", events.size(), ids.size(),
				rawMode ? "raw" : "semantic"));

		timed(() -> writeResults(replacement.apply(events, ids), resultFile, mappingFile, withProgressBar(ids.size())),
				"done");

	}

	private static BiFunction<List<String>, List<String>, Stream<Entry<String, String>>> replacer(boolean rawMode) {
		return rawMode ? Karp::rawReplacement : Karp::semanticReplacement;
	}

	private static Stream<Entry<String, String>> rawReplacement(List<String> events, List<String> allIds) {
		return allIds	.parallelStream()
						.map(rawUUID -> findReplacement(rawUUID, events))
						.flatMap(Optional<Entry<String, String>>::stream)
						.map(line -> replace(line));
	}

	private static Stream<Entry<String, String>> semanticReplacement(List<String> events, List<String> allIds) {
		return allIds	.parallelStream()
						.map(rawUUID -> idPattern(rawUUID))
						.map(pattern -> findReplacement(pattern, events))
						.flatMap(Optional<String>::stream)
						.map(line -> replace(line));
	}

	private static Optional<String> findReplacement(Pattern idRex, List<String> events) {
		return events	.parallelStream()
						.filter(value -> idRex	.matcher(value)
												.find())
						.findAny();
	}

	private static Optional<Entry<String, String>> findReplacement(String id, List<String> events) {
		return events	.parallelStream()
						.filter(value -> value.contains(id))
						.map(value -> Map.entry(id, value))
						.findAny();
	}

	private static Pattern idPattern(String id) {
		return Pattern.compile(EVENT_ID_REX_TEMPLATE.replaceFirst(EVENT_ID_STUB, id));
	}

	/**
	 * Replace used by the semantic mode.
	 * 
	 * Sometimes events have {@code event_id}duplicated, e.g. in
	 * {@code event_metadata} and {@code transport_metadata}. Additionally, id value
	 * can be duplicated under different name, e.g. {@code event_id} and
	 * {@code triggering_system_event_id}. Above cases depend on where in the platform
	 * data is captured and which system created original incoming event. Taking
	 * this into consideration, or when aiming at more generic use case, consider
	 * simply replacing by id value in the whole string.
	 */
	private static Entry<String, String> replace(String event) {
		Matcher matcher = EVETN_ID_PATTERN.matcher(event);
		if (!matcher.find())
			throw new RuntimeException("Can't match event id in " + event);

		String match = matcher.group(0);
		String oldId = matcher.group(1);
		String newId = UUID	.randomUUID()
							.toString();
		String wholeReplacement = match.replaceFirst(oldId, newId);
		return Map.entry(newId, matcher.replaceAll(wholeReplacement));
	}

	private static Entry<String, String> replace(Entry<String, String> replacement) {
		String oldID = replacement.getKey();
		String event = replacement.getValue();
		String newId = UUID	.randomUUID()
							.toString();
		return Map.entry(newId, event.replaceAll(oldID, newId));
	}

	private static void readDataFolder(Path path, List<String> data) {
		System.out.println("\n = " + path.toAbsolutePath());
		timed(() -> readAllFiles(path, data), "total");
	}

	private static void readAllFiles(Path dataFolder, List<String> allLines) {
		try (Stream<Path> paths = Files.walk(dataFolder)) {
			paths	.parallel()
					.filter(path -> path.toFile()
										.isFile())
					.forEach(path -> timed(() -> readFileIntoList(path, allLines), "  - " + path.getFileName()));
		} catch (IOException ioe) {
			throw new RuntimeException("Cannot read " + dataFolder, ioe);
		}
	}

	private static void readFileIntoList(Path source, List<String> allLines) {
		try (BufferedReader reader = Files.newBufferedReader(source, StandardCharsets.UTF_8)) {
			for (;;) {
				String line = reader.readLine();
				if (line == null)
					break;
				allLines.add(line);
			}
		} catch (IOException ex) {
			throw new RuntimeException("Cannot read " + source, ex);
		}
	}

	private static void writeResults(Stream<Entry<String, String>> generated, Path newEventsPath, Path newIdsPath,
			Runnable callback) {
		try (FileOutputStream newEvents = new FileOutputStream(newEventsPath.toFile(), true);
				FileOutputStream newIds = new FileOutputStream(newIdsPath.toFile(), true)) {
			generated.forEach(idToEvent -> {
				try {
					newIds.write(format("%s%n", idToEvent.getKey()).getBytes());
					newEvents.write(format("%s%n", idToEvent.getValue()).getBytes());
					callback.run();
				} catch (IOException ioe) {
					throw new RuntimeException("Cannot write bytes to file stream.", ioe);
				}
			});

		} catch (IOException ioe) {
			throw new RuntimeException("Cannot open file stream.", ioe);
		}
	}

	private static void cleanDir(Path resDirPath) {
		try {
			if (Files.exists(resDirPath)) {
				System.out.println("Cleaning " + resDirPath);
				try (Stream<Path> fileStream = Files.walk(resDirPath)) {
					fileStream	.sorted(Comparator.reverseOrder())
								.map(Path::toFile)
								.forEach(File::delete);
				}
			}
			File created = Files.createDirectories(resDirPath)
								.toFile();
			if (!created.exists())
				throw new RuntimeException("Missing " + resDirPath);
		} catch (IOException ioe) {
			throw new RuntimeException("Cannot recreate " + resDirPath, ioe);
		}
	}

	private static void createFile(Path filePath) {
		try {
			File outputFile = filePath.toFile();
			boolean created = outputFile.createNewFile();
			if (!created)
				throw new RuntimeException("Abort, don't want to overwrite " + outputFile.getAbsolutePath());

			System.out.println("Created output file " + outputFile.getAbsolutePath());
		} catch (IOException ioe) {
			throw new RuntimeException("Cannot create " + filePath, ioe);
		}
	}

	private static void timed(Runnable runnable, String msg) {
		Instant start = Instant.now();
		runnable.run();
		Instant end = Instant.now();
		Duration duration = Duration.between(start, end);
		System.out.println(format("%s %sd %sh %sm %ss %sms %sns", msg, duration.toDaysPart(), duration.toHoursPart(),
				duration.toMinutesPart(), duration.toSecondsPart(), duration.toMillisPart(), duration.toNanosPart()));
	}

	private static Runnable withProgressBar(int total) {
		final AtomicInteger done = new AtomicInteger();
		return () -> printProgress(done.incrementAndGet(), total);
	}

	private static void printProgress(int done, int total) {
		if (done > total) {
			throw new IllegalArgumentException();
		}

		int size = 80;
		int donePercents = (100 * done) / total;
		int doneLength = size * donePercents / 100;

		System.out.print(format("\r[%s%s] %s%% [(\u25B2%s \u25BC%s)/%s]", "#".repeat(doneLength),
				".".repeat(size - doneLength), donePercents, (total - done), done, total));

		if (done == total) {
			System.out.print("\n");
		}
	}

}
