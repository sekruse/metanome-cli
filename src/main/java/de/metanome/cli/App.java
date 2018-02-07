package de.metanome.cli;

import static java.util.stream.Collectors.toList;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.metanome.MetacrateResultReceiver;
import de.hpi.isg.profiledb.ProfileDB;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import de.metanome.algorithm_integration.Algorithm;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementDatabaseConnection;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingDatabaseConnection;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingTableInput;
import de.metanome.algorithm_integration.configuration.DbSystem;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.input.TableInputGenerator;
import de.metanome.algorithm_integration.result_receiver.OmniscientResultReceiver;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.input.database.DefaultDatabaseConnectionGenerator;
import de.metanome.backend.input.database.DefaultTableInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.metanome.backend.result_receiver.ResultPrinter;

import java.io.*;
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
        System.out.printf("Running %s\n", parameters.algorithmClassName);
        System.out.printf("* in:            %s\n", parameters.inputDatasets);
        System.out.printf("* out:           %s\n", parameters.output);
        System.out.printf("* configuration: %s\n", parameters.algorithmConfigurationValues);

        System.out.println("Initializing algorithm.");
        Experiment experiment = null;
        if (parameters.profileDbKey != null && parameters.profileDbLocation != null) {
            // Create an experiment.
            Subject subject = new Subject(parameters.algorithmClassName, "?");
            experiment = new Experiment(
                    parameters.profileDbKey,
                    subject,
                    parameters.profileDbTags.toArray(new String[0])
            );
        }
        OmniscientResultReceiver resultReceiver = createResultReceiver(parameters);
        Algorithm algorithm = configureAlgorithm(parameters, resultReceiver, experiment);

        TempFileGenerator tempFileGenerator = setUpTempFileGenerator(parameters, algorithm);

        final long startTimeMillis = System.currentTimeMillis();
        long elapsedMillis;
        boolean isExecutionSuccess = false;
        try {
            algorithm.execute();
            isExecutionSuccess = true;
        } catch (Exception e) {
            System.err.printf("Algorithm crashed.\n");
            e.printStackTrace();
        } finally {
            if (resultReceiver instanceof MetacrateResultReceiver) {
                try {
                    ((MetacrateResultReceiver) resultReceiver).getMetadataStore().flush();
                } catch (Exception e) {
                    System.err.println("Could not flush Metacrate.");
                    e.printStackTrace();
                }
            }

            if (tempFileGenerator != null) {
                tempFileGenerator.cleanUp();
            }

            long endTimeMillis = System.currentTimeMillis();
            elapsedMillis = endTimeMillis - startTimeMillis;
            System.out.printf("Elapsed time: %s (%d ms).\n", formatDuration(elapsedMillis), elapsedMillis);
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
            case "crate":
                if (resultReceiver instanceof MetacrateResultReceiver) {
                    try {
                        MetacrateResultReceiver metacrateResultReceiver = (MetacrateResultReceiver) resultReceiver;
                        metacrateResultReceiver.close();
                        break;
                    } catch (Exception e) {
                        System.err.println("Storing the result failed.");
                        e.printStackTrace();
                        System.exit(4);
                    }
                }
            case "file!":
            case "file":
            case "none":
                try {
                    if (resultReceiver instanceof Closeable) {
                        ((Closeable) resultReceiver).close();
                    }
                } catch (IOException e) {
                    System.err.println("Storing the result failed.");
                    e.printStackTrace();
                    System.exit(4);
                }
                break;
        }

        if (isExecutionSuccess && experiment != null) {
            // Register additional configuration.
            for (String spec : parameters.profileDbConf) {
                int colonIndex = spec.indexOf(':');
                if (colonIndex != -1) {
                    experiment.getSubject().addConfiguration(spec.substring(0, colonIndex), spec.substring(colonIndex + 1));
                }
            }

            // Register measured time.
            TimeMeasurement timeMeasurement = new TimeMeasurement("execution-millis");
            timeMeasurement.setMillis(elapsedMillis);
            experiment.addMeasurement(timeMeasurement);

            // Store the experiment.
            try {
                new ProfileDB().append(new File(parameters.profileDbLocation), experiment);
            } catch (IOException e) {
                System.err.printf("Could not store ProfileDB experiment: %s\n", e.getMessage());
                e.printStackTrace();
            }
        }

        System.exit(isExecutionSuccess ? 0 : 23);
    }

    private static OmniscientResultReceiver createResultReceiver(Parameters parameters) {
        String executionId;
        if (parameters.output.equalsIgnoreCase("none")) {
            try {
                return new DiscardingResultReceiver();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (parameters.output.startsWith("crate:")) {
            int lastColonIndex = parameters.output.lastIndexOf(":");
            if (lastColonIndex == "crate:".length() - 1) {
                throw new IllegalArgumentException(String.format("Could not parse output \"%s\".", parameters.output));
            }
            String scopeIdentifier = parameters.output.substring(lastColonIndex + 1);
            String cratePath = parameters.output.substring("crate:".length(), lastColonIndex);
            MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();
            metadataStoreParameters.metadataStore = cratePath;
            MetadataStore metadataStore = MetadataStoreUtil.loadMetadataStore(metadataStoreParameters);
            Target scope = metadataStore.getTargetByName(scopeIdentifier);
            if (scope == null) {
                throw new IllegalArgumentException("No such schema element: \"" + scopeIdentifier + "\".");
            }
            int schemaId = metadataStore.getIdUtils().getSchemaId(scope.getId());
            Schema schema = metadataStore.getSchemaById(schemaId);
            return new MetacrateResultReceiver(
                    metadataStore,
                    schema,
                    Collections.singleton(scope),
                    String.format("%s (%s, %s)", parameters.algorithmClassName, new Date(), "%s")
            );
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
     * @param experiment     a ProfileDB {@link Experiment} or {@code null}
     * @return the configured {@link Algorithm} instance
     */
    private static Algorithm configureAlgorithm(Parameters parameters, OmniscientResultReceiver resultReceiver, Experiment experiment) {
        try {
            final Algorithm algorithm = createAlgorithm(parameters.algorithmClassName);
            loadMiscConfigurations(parameters, algorithm, experiment);
            setUpInputGenerators(parameters, algorithm, experiment);
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

    private static void loadMiscConfigurations(Parameters parameters, Algorithm algorithm, Experiment experiment) throws AlgorithmConfigurationException {
        for (String algorithmConfigurationValue : parameters.algorithmConfigurationValues) {
            int colonPos = algorithmConfigurationValue.indexOf(':');
            final String key = algorithmConfigurationValue.substring(0, colonPos);
            final String value = algorithmConfigurationValue.substring(colonPos + 1);

            Boolean booleanValue = tryToParseBoolean(value);
            if (algorithm instanceof BooleanParameterAlgorithm && booleanValue != null) {
                ((BooleanParameterAlgorithm) algorithm).setBooleanConfigurationValue(key, booleanValue);
                if (experiment != null) experiment.getSubject().addConfiguration(key, booleanValue);
                continue;
            }

            Integer intValue = tryToParseInteger(value);
            if (algorithm instanceof IntegerParameterAlgorithm && intValue != null) {
                ((IntegerParameterAlgorithm) algorithm).setIntegerConfigurationValue(key, intValue);
                if (experiment != null) experiment.getSubject().addConfiguration(key, intValue);
                continue;
            }

            if (algorithm instanceof StringParameterAlgorithm) {
                ((StringParameterAlgorithm) algorithm).setStringConfigurationValue(key, value);
                if (experiment != null) experiment.getSubject().addConfiguration(key, value);
                continue;
            }

            System.err.printf("Could not set up configuration value \"%s\".\n", key);
        }

        if (experiment != null && algorithm instanceof ExperimentParameterAlgorithm) {
            ((ExperimentParameterAlgorithm) algorithm).setProfileDBExperiment(experiment);
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

    private static void setUpInputGenerators(Parameters parameters, Algorithm algorithm, Experiment experiment) throws AlgorithmConfigurationException {
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


            if (algorithm instanceof DatabaseConnectionParameterAlgorithm) {
                final List<ConfigurationRequirement<?>> db = algorithm.getConfigurationRequirements().stream()
                    .filter(cr -> cr.getClass() == ConfigurationRequirementDatabaseConnection.class)
                    .collect(toList());

                if (db.isEmpty()) {
                    System.err.println("DatabaseConnection not specified");
                } else {
                    Preconditions.checkState(db.size() == 1, "More than one DB conf requirement");

                    List<DatabaseConnectionGenerator> inputGenerators = new LinkedList<>();
                    for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                        inputGenerators.addAll(
                            createDatabaseConnectionGenerators(parameters, i, databaseSettings));
                    }
                    ((DatabaseConnectionParameterAlgorithm) algorithm)
                        .setDatabaseConnectionGeneratorConfigurationValue(db.get(0).getIdentifier(),
                            inputGenerators
                                .toArray(new DatabaseConnectionGenerator[inputGenerators.size()])
                        );
                }
            }

        } else {
            // We assume that we are given file inputs.
            if (algorithm instanceof RelationalInputParameterAlgorithm) {
                List<RelationalInputGenerator> inputGenerators = new LinkedList<>();
                for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                    inputGenerators.addAll(createFileInputGenerators(parameters, i, RelationalInputGenerator.class));
                }
                ((RelationalInputParameterAlgorithm) algorithm).setRelationalInputConfigurationValue(
                        parameters.inputDatasetKey,
                        inputGenerators.toArray(new RelationalInputGenerator[inputGenerators.size()])
                );

            } else {
                boolean isAnyInput = false;
                if (algorithm instanceof FileInputParameterAlgorithm) {
                    List<FileInputGenerator> inputGenerators = new LinkedList<>();
                    for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                        inputGenerators.addAll(createFileInputGenerators(parameters, i, FileInputGenerator.class));
                    }
                    ((FileInputParameterAlgorithm) algorithm).setFileInputConfigurationValue(
                            parameters.inputDatasetKey,
                            inputGenerators.toArray(new FileInputGenerator[inputGenerators.size()])
                    );
                    isAnyInput = true;
                }

                if (algorithm instanceof HdfsInputParameterAlgorithm) {
                    List<HdfsInputGenerator> inputGenerators = new LinkedList<>();
                    for (int i = 0; i < parameters.inputDatasets.size(); i++) {
                        inputGenerators.addAll(createFileInputGenerators(parameters, i, HdfsInputGenerator.class));
                    }
                    ((HdfsInputParameterAlgorithm) algorithm).setHdfsInputConfigurationValue(
                            parameters.inputDatasetKey,
                            inputGenerators.toArray(new HdfsInputGenerator[inputGenerators.size()])
                    );
                    isAnyInput = true;

                }

                if (!isAnyInput) {
                    System.err.printf("Algorithm does not implement a supported input method (relational/files).\n");
                    System.exit(5);
                    return;
                }
            }
        }

        if (experiment != null) {
            experiment.getSubject().addConfiguration(parameters.inputDatasetKey, parameters.inputDatasets);
        }
    }


    /**
     * Create a {@link DefaultFileInputGenerator}s.
     *
     * @param parameters     defines how to configure the {@link DefaultFileInputGenerator}
     * @param parameterIndex index of the dataset parameter to create the {@link DefaultFileInputGenerator}s for
     * @param cls            create {@link RelationalInputGenerator}s must be a subclass
     * @return the {@link DefaultFileInputGenerator}s
     * @throws AlgorithmConfigurationException
     */
    private static <T extends RelationalInputGenerator> Collection<T> createFileInputGenerators(
            Parameters parameters, int parameterIndex, Class<T> cls
    ) throws AlgorithmConfigurationException {
        final String parameter = parameters.inputDatasets.get(parameterIndex);
        if (parameter.startsWith("load:")) {
            try {
                return Files.lines(Paths.get(parameter.substring("load:".length())))
                        .map(path -> {
                            try {
                                return createFileInputGenerator(parameters, path, cls);
                            } catch (AlgorithmConfigurationException e) {
                                throw new RuntimeException("Could not create input generator.", e);
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(toList());
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
            T inputGenerator = createFileInputGenerator(parameters, parameter, cls);
            return inputGenerator == null ? Collections.emptyList() : Collections.singleton(inputGenerator);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends RelationalInputGenerator> T createFileInputGenerator(
            Parameters parameters, String path, Class<T> cls
    ) throws AlgorithmConfigurationException {
        ConfigurationSettingFileInput setting = new ConfigurationSettingFileInput(
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
        );
        RelationalInputGenerator generator;
        if (path.startsWith("hdfs://")) {
            generator = new HdfsInputGenerator(setting);
        } else {
            generator = new DefaultFileInputGenerator(setting);
        }
        if (!cls.isAssignableFrom(generator.getClass())) {
            return null;
        }
        return (T) generator;
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
                        .collect(toList());
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

    /**
     * Create a {@link DefaultDatabaseConnectionGenerator}s.
     *
     * @param parameters     defines how to configure the {@link DefaultDatabaseConnectionGenerator}
     * @param parameterIndex index of the dataset parameter to create the {@link DefaultDatabaseConnectionGenerator}s for
     * @return the {@link DefaultFileInputGenerator}s
     * @throws AlgorithmConfigurationException
     */
    private static Collection<DefaultDatabaseConnectionGenerator> createDatabaseConnectionGenerators(
        Parameters parameters,
        int parameterIndex,
        ConfigurationSettingDatabaseConnection databaseSettings) throws AlgorithmConfigurationException {
        final String parameter = parameters.inputDatasets.get(parameterIndex);
        if (parameter.startsWith("load:")) {
            try {
                return Files.lines(Paths.get(parameter.substring("load:".length())))
                    .map(table -> {
                        try {
                            return createDatabaseConnectionGenerator(databaseSettings, table);
                        } catch (AlgorithmConfigurationException e) {
                            throw new RuntimeException("Could not create input generator.", e);
                        }
                    })
                    .collect(toList());
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
                createDatabaseConnectionGenerator(databaseSettings, parameter)
            );
        }
    }

    private static DefaultDatabaseConnectionGenerator createDatabaseConnectionGenerator(
        ConfigurationSettingDatabaseConnection configurationSettingDatabaseConnection, String table) throws AlgorithmConfigurationException {
        return new DefaultDatabaseConnectionGenerator(new ConfigurationSettingDatabaseConnection(
            configurationSettingDatabaseConnection.getDbUrl(),
            configurationSettingDatabaseConnection.getUsername(),
            configurationSettingDatabaseConnection.getPassword(),
            configurationSettingDatabaseConnection.getSystem()
        ));
    }

    private static char toChar(String string) {
        if (string == null || string.isEmpty()) return '\0';
        else if (string.length() == 1) return string.charAt(0);
        switch (string) {
            case "none":
                return '\0';
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
            case "double":
                return '"';
            case "single":
                return '\'';
            default:
                throw new IllegalArgumentException(String.format("Illegal character specification: %s", string));
        }
    }

    public static TempFileGenerator setUpTempFileGenerator(final Parameters parameters, final Algorithm algorithm) {
        if (algorithm instanceof TempFileAlgorithm) {
            final TempFileGenerator generator = new TempFileGenerator(algorithm.getClass().getSimpleName(),
                parameters.tempFileDirectory,
                parameters.clearTempFiles,
                parameters.clearTempFilesByPrefix);
          ((TempFileAlgorithm) algorithm).setTempFileGenerator(generator);
          return generator;
        }

        return null;
    }

    public static void configureResultReceiver(Algorithm algorithm, OmniscientResultReceiver resultReceiver) {
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

        if (algorithm instanceof MetacrateClient && resultReceiver instanceof MetacrateResultReceiver) {
            ((MetacrateClient) algorithm).setMetadataStore(((MetacrateResultReceiver) resultReceiver).getMetadataStore());
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

        @Parameter(names = "--temp", description = "directory for temporary files")
        public String tempFileDirectory;

        @Parameter(names = "--clearTempFiles", description = "clear temporary files")
        public boolean clearTempFiles = true;

        @Parameter(names = "--clearTempFilesByPrefix", description =  "if additional files in the temp directory with same prefix should be removed")
        public boolean clearTempFilesByPrefix = false;

        @Parameter(names = {"-o", "--output"}, description = "how to output results (none/print/file[:run-ID]/crate:file:scope)")
        public String output = "file";

        @Parameter(names = "--profiledb-key", description = "experiment key to store a ProfileDB experiment")
        public String profileDbKey;

        @Parameter(names = "--profiledb-tags", description = "tags to store with a ProfileDB experiment", variableArity = true)
        public List<String> profileDbTags = new LinkedList<>();

        @Parameter(names = "--profiledb-conf", description = "additional configuration to store with a ProfileDB experiment", variableArity = true)
        public List<String> profileDbConf = new LinkedList<>();

        @Parameter(names = "--profiledb", description = "location of a ProfileDB to store a ProfileDB experiment at")
        public String profileDbLocation;

    }
}
