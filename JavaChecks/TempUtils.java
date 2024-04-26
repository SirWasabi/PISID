import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TempUtils {

    public static DateTimeFormatter DATEFORMAT = null;

    private double lastmed = 9999;
    private double outlierGap = 2.5;
    private double tempIdeal;
    private double varTemp;
    private double gap;


        public String[] dataString(String medicao){
        String[] par = medicao.split(",");
        String[] aux = {"", "", ""};
        for(int i = 0; i != par.length; i++)
            aux[i] = par[i].split(":")[1];
        return aux;
    }

    // data, leitura, sensor
    public boolean checkErrorValue(String medicao) {
        String[] par = medicao.split(",");
        String[] aux = {"", "", ""};

        for(int i = 0; i != par.length; i++)
            aux[i] = par[i].split(":")[1];

        try {
            LocalDateTime.parse(aux[0], DATEFORMAT);
            Double.parseDouble(aux[1]);
            Integer.parseInt(aux[2]);
        } catch (NumberFormatException e) {
            return false;
        }

        return (Integer.parseInt(aux[2]) == 1 || Integer.parseInt(aux[2]) == 2);
    }

    public boolean checkOutlier(double med) {
        if(lastmed == 9999) {
            lastmed = med;
            return true;
        }

        if (Math.abs(med - lastmed) <= outlierGap)
            lastmed = med;
            
        return(Math.abs(med - lastmed) <= outlierGap);
    }

    public boolean checkMedicao(String str) {
        if (checkErrorValue(str)) {
            return false;
        } else 
            return(checkOutlier(Double.parseDouble(str.split(",")[1].split(":")[1])));
    }

        private void processDataArray(List<String> dataArray) {
            for (String data : dataArray) {
                if(checkMedicao(dataArray)){
                    String aux = dataString(dataArray);
                    double tempAtual = Double.parseDouble(aux[1]);
                    if(tempAtual > tempIdeal + gap * varTemp || tempAtual < tempIdeal - gap * varTemp){
                        Alerta intermedio = createAlertaIntermedio(aux[0], aux[2], aux[1]);
                        System.out.println(intermedio);   //mandar sql
                    }
                    if(tempAtual > tempIdeal + varTemp || tempAtual < tempIdeal + varTemp){
                        Alerta critico = createAlertaCritico(aux[0], aux[2], aux[1]);
                        System.out.println(critico);     //mandar sql
                    }
                    System.out.println(dataArray);     //mandar para o sql caso nao haja problemas
                }else{
                    String[] aux = dataString(dataArray);
                    Alerta avaria = createAlertaAvaria(aux[0], aux[2], aux[1]);
                    System.out.println(avaria); //mandar para o sql depois
                }

            }
        }

    public static void main(String[] args) {
    }

    public Alerta createAlertaAvaria(String datetime, String sensor, String leitura) {
        return new Alerta(datetime, "", sensor, leitura, "Avaria", "Ocorreu um erro de escrita desta medição.");
    }

    public Alerta createAlertaIntermedio(String datetime, String sensor, String leitura) {
        return new Alerta(datetime, "", sensor, leitura, "Temperatura Intermédio", "A Temperatura do sensor " + sensor + " está perto de exceder os limites.");
    }

    public Alerta createAlertaCritico(String datetime, String sensor, String leitura) {
        return new Alerta(datetime, "", sensor, leitura, "Temperatura Crítico", "A Temperatura do sensor " + sensor + " excedeu os limites, abortar Experiência.");
    }

}
