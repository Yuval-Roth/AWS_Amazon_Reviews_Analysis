import java.util.*;

public class TablePrinter {
    List<String> columnNames;
    Map<String,List<String>> columns;
    Map<String,Integer> columnWidths;

    public TablePrinter (String... columnNames){
        this.columnNames = Arrays.asList(columnNames);
        this.columns = new HashMap<>();
        this.columnWidths = new HashMap<>();
        for (String column : columnNames) {
            columns.put(column, new LinkedList<>());
            columnWidths.put(column, column.length());
        }
    }

    public void addEntry(String... entries) {

        if (entries.length != this.columnNames.size()) {
            throw new IllegalArgumentException("Number of entries does not match number of columns");
        }

        int i = 0;
        for (String column : columnNames) {
            columns.get(column).add(entries[i]);
            if (columnWidths.get(column) < entries[i].length()) {
                columnWidths.put(column, entries[i].length());
            }
            i++;
        }
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();

        // create iterators for each column
        List<Iterator<String>> iterators = columnNames.stream()
                .map(c -> columns.get(c).iterator())
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        // create padding for each column
        String[] paddings = columnNames.stream()
                .map(c -> " ".repeat(columnWidths.get(c)))
                .toArray(String[]::new);

        // calculate total width of table for top and bottom borders
        int totalWidth = columnNames.stream()
                .mapToInt(c -> columnWidths.get(c))
                .sum() + 3 * columnNames.size() - 1;

        sb.append("-".repeat(totalWidth)).append("\n"); // top border

        //print headers
        for (int i = 0; i < columnNames.size(); i++) {
            // get next value and padding adjusted for value length
            String name = columnNames.get(i);
            String padding = paddings[i].substring(name.length());
            // build the row
            sb.append(name).append(padding).append(" | ");
        }
        sb.append("\n");
        sb.append("-".repeat(totalWidth-1)).append("|").append("\n"); // mid border

        //print rows
        while (iterators.getFirst().hasNext()){
            for(int i = 0; i < columnNames.size(); i++){

                // get next value and padding adjusted for value length
                String next = iterators.get(i).next();
                String padding = paddings[i].substring(next.length());

                // build the row
                sb.append(next).append(padding).append(" | ");
            }
            sb.append("\n"); // end of row
        }

        sb.append("-".repeat(totalWidth)); // bottom border

        return sb.toString();
    }
}
