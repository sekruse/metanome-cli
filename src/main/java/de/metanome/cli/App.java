package de.metanome.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.metanome.algorithm_integration.Algorithm;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

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
        ResultCache resultCache = createResultReceiver(parameters);
        Algorithm algorithm = configureAlgorithm(parameters, resultCache);

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
        switch (parameters.output.split(":")[0]) {
            case "print":
                System.out.println("Results:");
                for (Result result : resultCache.fetchNewResults()) {
                    System.out.println(result);
                }
                break;
            default:
                System.out.printf("Unknown output mode \"%s\". Defaulting to \"file\"\n", parameters.output);
            case "file":
                try {
                    resultCache.close();
                } catch (IOException e) {
                    System.err.println("Storing the result failed.");
                    e.printStackTrace();
                    System.exit(4);
                }
                break;
            case "none":
                break;
        }
    }

    private static ResultCache createResultReceiver(Parameters parameters) {
        String executionId;
        if (parameters.output.startsWith("file:")) {
            executionId = parameters.output.substring("file:".length());
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
        }
        try {
            return new ResultCache(executionId, null);
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
     * @param parameters  tell which {@link Algorithm} to instantiate and provides its properties.
     * @param resultCache that should be used by the {@link Algorithm} to store results
     * @return the configured {@link Algorithm} instance
     */
    private static Algorithm configureAlgorithm(Parameters parameters, ResultCache resultCache) {
        final Algorithm algorithm;
        try {
            algorithm = createAlgorithm(parameters.algorithmClassName);
            loadMiscConfigurations(parameters, algorithm);
            loadFileInputGenerators(parameters, algorithm);
            configureResultReceiver(algorithm, resultCache);
        } catch (Exception e) {
            System.err.println("Could not initialize algorithm.");
            e.printStackTrace();
            System.exit(3);
            return null;
        }

        return algorithm;
    }

    private static Algorithm createAlgorithm(String algorithmClassName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final Class<?> algorithmClass = Class.forName(algorithmClassName);
        return (Algorithm) algorithmClass.newInstance();
    }

    private static void loadMiscConfigurations(Parameters parameters, Algorithm algorithm) throws AlgorithmConfigurationException {
        for (String algorithmConfigurationValue : parameters.algorithmConfigurationValues) {
            final String[] keyValuePair = algorithmConfigurationValue.split(":");
            final String key = keyValuePair[0];
            final String value = keyValuePair[1];

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

    private static void loadFileInputGenerators(Parameters parameters, Algorithm algorithm) throws AlgorithmConfigurationException {
        if (algorithm instanceof RelationalInputParameterAlgorithm) {
            RelationalInputGenerator[] inputGenerators = new RelationalInputGenerator[parameters.inputDatasets.size()];
            for (int i = 0; i < inputGenerators.length; i++) {
                inputGenerators[i] = createInputGenerator(parameters, i);
            }
            ((RelationalInputParameterAlgorithm) algorithm).setRelationalInputConfigurationValue(parameters.inputDatasetKey, inputGenerators);

        } else if (algorithm instanceof FileInputParameterAlgorithm) {
            FileInputGenerator[] inputGenerators = new FileInputGenerator[parameters.inputDatasets.size()];
            for (int i = 0; i < inputGenerators.length; i++) {
                inputGenerators[i] = createInputGenerator(parameters, i);
            }
            ((FileInputParameterAlgorithm) algorithm).setFileInputConfigurationValue(parameters.inputDatasetKey, inputGenerators);

        } else {
            System.err.printf("Algorithm does not implement a supported input method (relational/files).\n");
            System.exit(5);
            return;
        }
    }

    /**
     * Create a new {@link DefaultFileInputGenerator}.
     * @param parameters defines how to configure the {@link DefaultFileInputGenerator}
     * @param datasetIndex index of the dataset to create the {@link DefaultFileInputGenerator} for
     * @return the {@link DefaultFileInputGenerator}
     * @throws AlgorithmConfigurationException
     */
    private static DefaultFileInputGenerator createInputGenerator(Parameters parameters, int datasetIndex) throws AlgorithmConfigurationException {
        return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                parameters.inputDatasets.get(datasetIndex),
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

    private static char toChar(String string) {
        return string == null || string.isEmpty() ? '\0' : string.charAt(0);
    }

    public static void configureResultReceiver(Algorithm algorithm, ResultCache resultReceiver) {
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

        @Parameter(names = "--file-key", description = "configuration key for the input files", required = true)
        public String inputDatasetKey;

        @Parameter(names = "--files", description = "input file to be analyzed", required = true, variableArity = true)
        public List<String> inputDatasets = new ArrayList<>();

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
