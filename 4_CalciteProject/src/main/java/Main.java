import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.io.IOException;
import java.util.*;

public class Main {

    private static final List<Object[]> drama;

    static {
        try {
            drama = (CSVReader.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final List<Object[]> drama_genres;

    static {
        try {
            drama_genres = (CSVReader2.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final List<Object[]> drama_cast;

    static {
        try {
            drama_cast = (CSVReader3.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static final List<Object[]> drama_watched;

    static {
        try {
            drama_watched = (CSVReader4.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //genres
    private static final List<Object[]> genres;

    static {
        try {
            genres = (CSVReader5.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final List<Object[]> review;

    static {
        try {
            review = (CSVReader6.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //user
    private static final List<Object[]> user;

    static {
        try {
            user = (CSVReader7.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //actor
    private static final List<Object[]> actor;

    static {
        try {
            actor = (CSVReader7.reader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws Exception {
        //new EndToEndExampleBindable().example();
        //new QueryStream().run();
        System.out.println(CSVReader.reader());
        System.out.println((drama));
        //System.out.println(drama_genres.getClass());
        //System.out.println(drama.getClass());
        new Main().queryProcessor();

    }

    public void queryProcessor() throws Exception{
        // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        // Create the root schema describing the data model
        CalciteSchema schema = CalciteSchema.createRootSchema(true);
        // Define type for drama_genres table
        RelDataTypeFactory.Builder drama_genreType = new RelDataTypeFactory.Builder(typeFactory);
        drama_genreType.add("dra_id", SqlTypeName.INTEGER);
        drama_genreType.add("gen_id", SqlTypeName.INTEGER);


        // Initialize authors table with data
        Main.ListTable drama_genreTable = new ListTable(drama_genreType.build(), (List<Object[]>) drama_genres);
        // Add authors table to the schema
        schema.add("drama_genres", drama_genreTable);
        // Define type for drama table
        RelDataTypeFactory.Builder dramaType = new RelDataTypeFactory.Builder(typeFactory);
        dramaType.add("dra_id", SqlTypeName.INTEGER);
        dramaType.add("dra_title", SqlTypeName.VARCHAR);
        dramaType.add("dra_year", SqlTypeName.INTEGER);
        dramaType.add("dra_time", SqlTypeName.VARCHAR);
        // Initialize drama table with data
        ListTable dramaTable = new ListTable(dramaType.build(), (List<Object[]>) drama);
        // Add books table to the schema
        schema.add("drama", dramaTable);

        // Define type drama_cast
        RelDataTypeFactory.Builder drama_CastType = new RelDataTypeFactory.Builder(typeFactory);
        drama_CastType.add("dra_id", SqlTypeName.INTEGER);
        drama_CastType.add("act_id", SqlTypeName.INTEGER);

        // Initialize books table with data
        ListTable drama_castTable = new ListTable(drama_CastType.build(), (List<Object[]>) drama_cast);
        // Add drama_cast table to the schema
        schema.add("drama_cast",drama_castTable);



        // Define type drama_watch
        RelDataTypeFactory.Builder drama_watchType = new RelDataTypeFactory.Builder(typeFactory);
        drama_watchType.add("user_id", SqlTypeName.INTEGER);
        drama_watchType.add("dra_id", SqlTypeName.INTEGER);

        // Initialize drama_watch table with data
        ListTable drama_watchedTable = new ListTable(drama_watchType.build(), (List<Object[]>) drama_watched);
        // Add drama_cast table to the schema
        schema.add("drama_watched",drama_watchedTable );



        // Define type genres
        RelDataTypeFactory.Builder genresType = new RelDataTypeFactory.Builder(typeFactory);
        genresType.add("gen_id", SqlTypeName.INTEGER);
        genresType.add("gen_title", SqlTypeName.VARCHAR);

        // Initialize genres table with data
        ListTable genresTable = new ListTable(genresType.build(), (List<Object[]>) genres);
        // Add drama_cast table to the schema
        schema.add("genres",genresTable );


        // Define type review
        RelDataTypeFactory.Builder reviewType = new RelDataTypeFactory.Builder(typeFactory);
        reviewType.add("user_id", SqlTypeName.INTEGER);
        reviewType.add("dra_id", SqlTypeName.INTEGER);
        reviewType.add("review", SqlTypeName.VARCHAR);

        // Initialize review table with data
        ListTable reviewTable = new ListTable(reviewType.build(), (List<Object[]>) review);
        // Add review table to the schema
        schema.add("review",reviewTable );

        // Define type user
        RelDataTypeFactory.Builder userType = new RelDataTypeFactory.Builder(typeFactory);
        userType.add("user_id", SqlTypeName.INTEGER);
        userType.add("name", SqlTypeName.VARCHAR);

        // Initialize user table with data
        ListTable userTable = new ListTable(userType.build(), (List<Object[]>) user);
        // Add review table to the schema
        schema.add("user",userTable );

        // Define type actor
        //act_id,Act_Name,Date_Birth,BPlace,Img,WikiLink
        RelDataTypeFactory.Builder actorType = new RelDataTypeFactory.Builder(typeFactory);
        actorType.add("act_id", SqlTypeName.INTEGER);
        actorType.add("Act_Name", SqlTypeName.VARCHAR);
        actorType.add("Date_Birth", SqlTypeName.DATE);
        actorType.add("BPlace", SqlTypeName.VARCHAR);
        actorType.add("Img", SqlTypeName.VARCHAR);
        actorType.add("WikiLink", SqlTypeName.VARCHAR);

        // Initialize user table with data
        ListTable actorTable = new ListTable(actorType.build(), (List<Object[]>) actor);
        // Add review table to the schema
        schema.add("actor",actorTable );



        // Create an SQL parser
        SqlParser parser = SqlParser.create(
                "SELECT act_id FROM actor limit 5");
        // Parse the query into an AST
        SqlNode sqlNode = parser.parseQuery();

        // Configure and instantiate validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                Collections.singletonList(""),
                typeFactory, config);

        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
        RelOptCluster cluster = newCluster(typeFactory);
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

        // Display the logical plan
       System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                       SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Initialize optimizer/planner with the necessary rules
        RelOptPlanner planner = cluster.getPlanner();

        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(Bindables.BINDABLE_FILTER_RULE);
        planner.addRule(Bindables.BINDABLE_JOIN_RULE);
        planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
        planner.addRule(Bindables.BINDABLE_SORT_RULE);

        // Define the type of the output plan (in this case we want a physical plan in
        // BindableConvention)
        logPlan = planner.changeTraits(logPlan,
                cluster.traitSet().replace(BindableConvention.INSTANCE));
        planner.setRoot(logPlan);
        // Start the optimization process to obtain the most efficient physical plan based on the
        // provided rule set.
        BindableRel phyPlan = (BindableRel) planner.findBestExp();

        // Display the physical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));

        // Run the executable plan using a context simply providing access to the schema
        for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema))) {
            //System.out.println(Arrays.toString(row));
            System.out.println(row);

        }
    }


    /**
     * A simple table based on a list.
     */

    static class ListTable extends AbstractTable implements ScannableTable {
        private final RelDataType rowType;
        private final List<Object[]> data;

        ListTable(RelDataType rowType, List<Object[]> data) {
            this.rowType = rowType;
            this.data = data;
        }

        @Override public Enumerable<Object[]> scan(final DataContext root) {
            return Linq4j.asEnumerable(data);
        }

        @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            return rowType;
        }
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
            , viewPath) -> null;

    /**
     * A simple data context only with schema information.
     */


    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override public QueryProvider getQueryProvider() {
            return null;
        }

        @Override public Object get(final String name) {
            return null;
        }
    }





}
