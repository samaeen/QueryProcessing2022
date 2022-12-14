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
        // Define type for authors table
        RelDataTypeFactory.Builder drama_genreType = new RelDataTypeFactory.Builder(typeFactory);
        drama_genreType.add("dra_id", SqlTypeName.INTEGER);
        drama_genreType.add("gen_id", SqlTypeName.INTEGER);


        // Initialize authors table with data
        Main.ListTable drama_genreTable = new ListTable(drama_genreType.build(), (List<Object[]>) drama_genres);
        // Add authors table to the schema
        schema.add("drama_genres", drama_genreTable);
        // Define type for books table
        RelDataTypeFactory.Builder dramaType = new RelDataTypeFactory.Builder(typeFactory);
        dramaType.add("dra_id", SqlTypeName.INTEGER);
        dramaType.add("dra_title", SqlTypeName.VARCHAR);
        dramaType.add("dra_year", SqlTypeName.INTEGER);
        dramaType.add("dra_time", SqlTypeName.VARCHAR);
        // Initialize books table with data
        ListTable booksTable = new ListTable(dramaType.build(), (List<Object[]>) drama);
        // Add books table to the schema
        schema.add("drama", booksTable);

        // Create an SQL parser
        SqlParser parser = SqlParser.create(
                "SELECT dra_title FROM drama limit 5");
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
            System.out.println(Arrays.toString(row));
            //System.out.println(row);

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
