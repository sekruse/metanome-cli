package de.metanome.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.metanome.algorithm_integration.Algorithm;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingDatabaseConnection;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingTableInput;
import de.metanome.algorithm_integration.configuration.DbSystem;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.input.database.DefaultTableInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.metanome.backend.result_receiver.ResultPrinter;
import de.metanome.backend.result_receiver.ResultReceiver;
import org.apache.lucene.util.mutable.MutableValueDate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * App to run Metanome algorithms from the command line.
 */
public class App {

    public static void main(String[] args) {
        final Parameters parameters = parseParameters(args);
        run(parameters);
    }

    private static Parameters parseParameters(String[] args) {
        final Parameters parameters = new Parameters();
        final JCommander jCommander = new JCommander(parameters);
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            System.err.println("Could not parse command line args: " + e.getMessage());
            StringBuilder sb = new StringBuilder();
            jCommander.usage(sb);
            System.err.println(sb.toString());
            System.exit(1);
        }
        return parameters;
    }

    private static void run(Parameters parameters) {
        System.out.println("Initializing algorithm.");
        ResultReceiver resultReceiver = createResultReceiver(parameters);
        Algorithm algorithm = configureAlgorithm(parameters, resultReceiver);

        final long startTimeMillis = System.currentTimeMillis();
        try {
            algorithm.execute();
        } catch (Exception e) {
            System.err.printf("Algorithm crashed.\n");
            e.printStackTrace();
        } finally {
            long endTimeMillis = System.currentTimeMillis();
            System.out.printf("Elapsed time: %s.\n", formatDuration(endTimeMillis - startTimeMillis));
        }

        // Handle "file:exec-id" formats properly.
        ResultCache resultCache;
        switch (parameters.output.split(":")[0]) {
            case "print":
                resultCache = (ResultCache) resultReceiver;
                System.out.println("Results:");
                for (Result result : resultCache.fetchNewResults()) {
                    System.out.println(result);
                }
                break;
            default:
                System.out.printf("Unknown output mode \"%s\". Defaulting to \"file\"\n", parameters.output);
            case "file!":
            case "file":
            case "none":
                try {
                    resultReceiver.close();
                } catch (IOException e) {
                    System.err.println("Storing the result failed.");
                    e.printStackTrace();
                    System.exit(4);
                }
                break;
        }
    }

    private static ResultReceiver createResultReceiver(Parameters parameters) {
        String executionId;
        if (parameters.output.equalsIgnoreCase("none")) {
            try {
                return new DiscardingResultReceiver();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        boolean isCaching;
        if (parameters.output.startsWith("file:")) {
            executionId = parameters.output.substring("file:".length());
            isCaching = true;
        } else if (parameters.output.startsWith("file!:")) {
            executionId = parameters.output.substring("file!:".length());
            isCaching = false;
        } else if (parameters.output.equalsIgnoreCase("file!")) {
            Calendar calendar = GregorianCalendar.getInstance();
            executionId = String.format("%04d-%02d-%02d_%02d-%02d-%02d",
                    calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH) + 1,
                    calendar.get(Calendar.DATE),
                    calendar.get(Calendar.HOUR_OF_DAY),
                    calendar.get(Calendar.MINUTE),
                    calendar.get(Calendar.SECOND)
            );
            isCaching = false;
        } else {
            Calendar calendar = GregorianCalendar.getInstance();
            executionId = String.format("%04d-%02d-%02d_%02d-%02d-%02d",
                    calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH) + 1,
                    calendar.get(Calendar.DATE),
                    calendar.get(Calendar.HOUR_OF_DAY),
                    calendar.get(Calendar.MINUTE),
                    calendar.get(Calendar.SECOND)
            );
            isCaching = true;
        }
        try {
            return isCaching ?
                    new ResultCache(executionId, null) :
                    new ResultPrinter(executionId, null);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unexpected exception.", e);
        }
    }

    private static String formatDuration(long millis) {
        if (millis < 0) return "-:--:--.---";
        long ms = millis % 1000;
        millis /= 1000;
        long s = millis % 60;
        millis /= 60;
        long m = millis % 60;
        millis /= 60;
        long h = millis % 60;
        return String.format("%d:%02d:%02d.%03d", h, m, s, ms);
    }

    /**
     * Instantiate and configure an {@link Algorithm} instance according to the {@link Parameters}.
     *
     * @param parameters     tell which {@link Algorithm} to instantiate and provides its properties.
     * @param resultReceiver that should be used by the {@link Algorithm} to store results
     * @return the configured {@link Algorithm} instance
     */
    private static Algorithm configureAlgorithm(Parameters parameters, ResultReceiver resultReceiver) {
        try {
            final Algorithm algorithm = createAlgorithm(parameters.algorithmClassName);
            loadMiscConfigurations(parameters, algorithm);
            setUpInputGenerators(parameters, algorithm);
            configureResultReceiver(algorithm, resultReceiver);
            return algorithm;

        } catch (Exception e) {
            System.err.println("Could not initialize algorithm.");
            e.printStackTrace();
            System.exit(3);
            return null;
        }
    }

    private static Algorithm createAlgorithm(String algorithmClassName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final Class<?> algorithmClass = Class.forName(algorithmClassName);
        return (Algorithm) algorithmClass.newInstance();
    }

    private static void loadMiscConfigurations(Parameters parameters, Algorithm algorithm) throws AlgorithmConfigurationException {
        for (String algorithmConfigurationValue : parameters.algorithmConfigurationValues) {
            int colonPos = algorithmConfigurationValue.indexOf(':');
            final String key = algorithmConfigurationValue.substring(0, colonPos);
            final String value = algorithmConfigurationValue.substring(colonPos + 1);

            Boolean booleanValue = tryToParseBoolean(value);
            if (algorithm instanceof BooleanParameterAlgorithm && booleanValue != null) {
                ((BooleanParameterAlgorithm) algorithm).setBooleanConfigurationValue(key, booleanValue);
                continue;
            }

            Integer intValue = tryToParseInteger(value);
            if (algorithm instanceof IntegerParameterAlgorithm && intValue != null) {
                ((IntegerParameterAlgorithm) algorithm).setIntegerConfigurationValue(key, intValue);
                continue;
            }

            if (algorithm instanceof StringParameterAlgorithm) {
                ((StringParameterAlgorithm) algorithm).setStringConfigurationValue(key, value);
                continue;
            }

            System.err.printf("Could not set up configuration value \"%s\".\n", key);
        }
    }

    private static Boolean tryToParseBoolean(String str) {
        if (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(str);
        } else {
            return null;
        }
    }

    private static Integer tryToParseInteger(String str) {
        try {
            return Integer.valueOf(str);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static void setUpInputGenerators(Parameters parameters, Algorithm algorithm) throws AlgorithmConfigurationException {
        if (parameters.pgpassPath != null) {
            // We assume that we are given table inputs.
            ConfigurationSettingDatabaseConnection databaseSettings = loadConfigurationSettingDatabaseConnection(
                    parameters.pgpassPath, parameters.dbType
            );
            if (algorithm instanceof RelationalInputParameterAlgorithm) {
                List<RelationalInputGenerator> inputGenerators = new LinkedList<>();
                for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                    inputGenerators.addAll(createTableInputGenerators(parameters, i, databaseSettings));
                }
                ((RelationalInputParameterAlgorithm) algorithm).setRelationalInputConfigurationValue(
                        parameters.inputDatasetKey,
                        inputGenerators.toArray(new RelationalInputGenerator[inputGenerators.size()])
                );

            } else if (algorithm instanceof TableInputParameterAlgorithm) {
                List<TableInputGenerator> inputGenerators = new LinkedList<>();
                for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                    inputGenerators.addAll(createTableInputGenerators(parameters, i, databaseSettings));
                }
                ((TableInputParameterAlgorithm) algorithm).setTableInputConfigurationValue(
                        parameters.inputDatasetKey,
                        inputGenerators.toArray(new TableInputGenerator[inputGenerators.size()])
                );

            } else {
                System.err.printf("Algorithm does not implement a supported input method (relational/tables).\n");
                System.exit(5);
                return;
            }

        } else {
            // We assume that we are given file inputs.
            if (algorithm instanceof RelationalInputParameterAlgorithm) {
                List<RelationalInputGenerator> inputGenerators = new LinkedList<>();
                for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                    inputGenerators.addAll(createFileInputGenerators(parameters, i));
                }
                ((RelationalInputParameterAlgorithm) algorithm).setRelationalInputConfigurationValue(
                        parameters.inputDatasetKey,
                        inputGenerators.toArray(new RelationalInputGenerator[inputGenerators.size()])
                );

            } else if (algorithm instanceof FileInputParameterAlgorithm) {
                List<FileInputGenerator> inputGenerators = new LinkedList<>();
                for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                    inputGenerators.addAll(createFileInputGenerators(parameters, i));
                }
                ((FileInputParameterAlgorithm) algorithm).setFileInputConfigurationValue(
                        parameters.inputDatasetKey,
                        inputGenerators.toArray(new FileInputGenerator[inputGenerators.size()])
                );

            } else {
                System.err.printf("Algorithm does not implement a supported input method (relational/files).\n");
                System.exit(5);
                return;
            }

        }
    }

    /**
     * Create a {@link DefaultFileInputGenerator}s.
     *
     * @param parameters     defines how to configure the {@link DefaultFileInputGenerator}
     * @param parameterIndex index of the dataset parameter to create the {@link DefaultFileInputGenerator}s for
     * @return the {@link DefaultFileInputGenerator}s
     * @throws AlgorithmConfigurationException
     */
    private static Collection<DefaultFileInputGenerator> createFileInputGenerators(Parameters parameters, int parameterIndex) throws AlgorithmConfigurationException {
        final String parameter = parameters.inputDatasets.get(parameterIndex);
        if (parameter.startsWith("load:")) {
            try {
                return Files.lines(Paths.get(parameter.substring("load:".length())))
                        .map(path -> {
                            try {
                                return createFileInputGenerator(parameters, path);
                            } catch (AlgorithmConfigurationException e) {
                                throw new RuntimeException("Could not create input generator.", e);
                            }
                        })
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException("Could not load input specification file.", e);
            } catch (RuntimeException e) {
                if (e.getCause() != null && (e.getCause() instanceof AlgorithmConfigurationException)) {
                    throw (AlgorithmConfigurationException) e.getCause();
                } else {
                    throw e;
                }
            }
        } else {
            return Collections.singleton(
                    createFileInputGenerator(parameters, parameter)
            );
        }
    }

    private static DefaultFileInputGenerator createFileInputGenerator(Parameters parameters, String path) throws AlgorithmConfigurationException {
        return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                path,
                true,
                toChar(parameters.inputFileSeparator),
                toChar(parameters.inputFileQuotechar),
                toChar(parameters.inputFileEscape),
                parameters.inputFileStrictQuotes,
                parameters.inputFileIgnoreLeadingWhiteSpace,
                parameters.inputFileSkipLines,
                parameters.inputFileHasHeader,
                parameters.inputFileSkipDifferingLines,
                parameters.inputFileNullString
        ));
    }

    private static ConfigurationSettingDatabaseConnection loadConfigurationSettingDatabaseConnection(
            String pgpassPath, String dbType) throws AlgorithmConfigurationException {
        try {
            String firstLine = Files.lines(new File(pgpassPath).toPath()).findFirst().orElseThrow(
                    () -> new AlgorithmConfigurationException("Could not load PGPass file.")
            );
            int colonPos1 = firstLine.indexOf(':');
            int colonPos2 = firstLine.indexOf(':', colonPos1 + 1);
            int colonPos3 = firstLine.indexOf(':', colonPos2 + 1);
            int colonPos4 = firstLine.indexOf(':', colonPos3 + 1);
            if (colonPos4 == -1) {
                throw new IllegalArgumentException("Cannot parse PGPass file.");
            }
            String host = firstLine.substring(0, colonPos1);
            String port = firstLine.substring(colonPos1 + 1, colonPos2);
            String dbName = firstLine.substring(colonPos2 + 1, colonPos3);
            String user = firstLine.substring(colonPos3 + 1, colonPos4);
            String password = firstLine.substring(colonPos4 + 1);

            // TODO: Consider special JDBC URL formats, such as Oracle Thin.
            String jdbcUrl = String.format("jdbc:%s://%s:%s/%s", dbType, host, port, dbName);
            DbSystem dbSystem = DbSystem.PostgreSQL;
            if ("postgres".equalsIgnoreCase(dbType)) {
                dbSystem = DbSystem.PostgreSQL;
            } else if ("mysql".equalsIgnoreCase(dbType)) {
                dbSystem = DbSystem.MySQL;
            } else {
                // TODO: Consider other DB types. But it does not seem that this is a crucial piece of information for Metanome.
            }
            return new ConfigurationSettingDatabaseConnection(
                    jdbcUrl, user, password, dbSystem
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Create a {@link DefaultTableInputGenerator}s.
     *
     * @param parameters     defines how to configure the {@link DefaultTableInputGenerator}
     * @param parameterIndex index of the dataset parameter to create the {@link DefaultTableInputGenerator}s for
     * @return the {@link DefaultFileInputGenerator}s
     * @throws AlgorithmConfigurationException
     */
    private static Collection<DefaultTableInputGenerator> createTableInputGenerators(
            Parameters parameters,
            int parameterIndex,
            ConfigurationSettingDatabaseConnection databaseSettings) throws AlgorithmConfigurationException {
        final String parameter = parameters.inputDatasets.get(parameterIndex);
        if (parameter.startsWith("load:")) {
            try {
                return Files.lines(Paths.get(parameter.substring("load:".length())))
                        .map(table -> {
                            try {
                                return createTableInputGenerator(databaseSettings, table);
                            } catch (AlgorithmConfigurationException e) {
                                throw new RuntimeException("Could not create input generator.", e);
                            }
                        })
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException("Could not load input specification file.", e);
            } catch (RuntimeException e) {
                if (e.getCause() != null && (e.getCause() instanceof AlgorithmConfigurationException)) {
                    throw (AlgorithmConfigurationException) e.getCause();
                } else {
                    throw e;
                }
            }
        } else {
            return Collections.singleton(
                    createTableInputGenerator(databaseSettings, parameter)
            );
        }
    }

    private static DefaultTableInputGenerator createTableInputGenerator(
            ConfigurationSettingDatabaseConnection configurationSettingDatabaseConnection, String table) throws AlgorithmConfigurationException {
        return new DefaultTableInputGenerator(new ConfigurationSettingTableInput(
                table, configurationSettingDatabaseConnection
        ));
    }

    private static char toChar(String string) {
        if (string == null || string.isEmpty()) return '\0';
        else if (string.length() == 1) return string.charAt(0);
        switch (string) {
            case "\\t":
            case "tab":
                return '\t';
            case "' '":
            case "\" \"":
            case "space":
                return ' ';
            case "semicolon":
                return ';';
            case "comma":
                return ',';
            case "|":
            case "pipe":
                return '|';
            default:
                throw new IllegalArgumentException(String.format("Illegal character specification: %s", string));
        }
    }

    public static void configureResultReceiver(Algorithm algorithm, ResultReceiver resultReceiver) {
        boolean isAnyResultReceiverConfigured = false;
        if (algorithm instanceof FunctionalDependencyAlgorithm) {
            ((FunctionalDependencyAlgorithm) algorithm).setResultReceiver(resultReceiver);
            isAnyResultReceiverConfigured = true;
        }

        if (algorithm instanceof InclusionDependencyAlgorithm) {
            ((InclusionDependencyAlgorithm) algorithm).setResultReceiver(resultReceiver);
            isAnyResultReceiverConfigured = true;
        }

        if (algorithm instanceof UniqueColumnCombinationsAlgorithm) {
            ((UniqueColumnCombinationsAlgorithm) algorithm).setResultReceiver(resultReceiver);
            isAnyResultReceiverConfigured = true;
        }

        if (algorithm instanceof BasicStatisticsAlgorithm) {
            ((BasicStatisticsAlgorithm) algorithm).setResultReceiver(resultReceiver);
            isAnyResultReceiverConfigured = true;
        }

        if (algorithm instanceof OrderDependencyAlgorithm) {
            ((OrderDependencyAlgorithm) algorithm).setResultReceiver(resultReceiver);
            isAnyResultReceiverConfigured = true;
        }

        if (algorithm instanceof MultivaluedDependencyAlgorithm) {
            ((MultivaluedDependencyAlgorithm) algorithm).setResultReceiver(resultReceiver);
            isAnyResultReceiverConfigured = true;
        }

        if (!isAnyResultReceiverConfigured) {
            System.err.println("Could not configure any result receiver.");
        }
    }

    /**
     * Parameters for the Metanome CLI {@link App}.
     */
    public static class Parameters {

        @Parameter(names = {"--algorithm-config"}, description = "algorithm configuration parameters (<name>:<value>)", variableArity = true)
        public List<String> algorithmConfigurationValues = new ArrayList<>();

        @Parameter(names = {"-a", "--algorithm"}, description = "name of the Metanome algorithm class", required = true)
        public String algorithmClassName;

        @Parameter(names = {"--file-key", "--input-key", "--table-key"}, description = "configuration key for the input files/tables", required = true)
        public String inputDatasetKey;

        @Parameter(names = {"--files", "--inputs", "--tables"}, description = "input file/tables to be analyzed and/or files list input files/tables (prefixed with 'load:')", required = true, variableArity = true)
        public List<String> inputDatasets = new ArrayList<>();

        @Parameter(names = "--db-connection", description = "a PGPASS file that specifies the database connection; if given, the inputs are treated as database tables", required = false)
        public String pgpassPath = null;

        @Parameter(names = "--db-type", description = "the type of database as it would appear in a JDBC URL", required = false)
        public String dbType = null;

        @Parameter(names = "--separator", description = "separates fields in the input file")
        public String inputFileSeparator = ";";

        @Parameter(names = "--quote", description = "delimits fields in the input file")
        public String inputFileQuotechar = "\"";

        @Parameter(names = "--escape", description = "escapes special characters")
        public String inputFileEscape = "\0";

        @Parameter(names = "--skip", description = "numbers of lines to skip")
        public int inputFileSkipLines = 0;

        @Parameter(names = "--strict-quotes", description = "enforce strict quotes")
        public boolean inputFileStrictQuotes = false;

        @Parameter(names = "--ignore-leading-spaces", description = "ignore leading white spaces in each field")
        public boolean inputFileIgnoreLeadingWhiteSpace = false;

        @Parameter(names = "--header", description = "first row is a header")
        public boolean inputFileHasHeader = false;

        @Parameter(names = "--skip-differing-lines", description = "skip lines with incorrect number of fields")
        public boolean inputFileSkipDifferingLines = false;

        @Parameter(names = "--null", description = "representation of NULLs")
        public String inputFileNullString = "";

        @Parameter(names = {"-o", "--output"}, description = "how to output results (none/print/file[:run-ID])")
        public String output = "file";

    }
}
