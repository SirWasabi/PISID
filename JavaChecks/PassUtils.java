import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class PassUtils {

    public static DateTimeFormatter DATEFORMAT = null;
    public static List<Corredor> CORREDORES;

    // data, leitura, sensor
    public boolean checkErrorValue(String medicao) {
        String[] par = medicao.split(",");
        String[] aux = {"", "", ""};

        for(int i = 0; i != par.length; i++)
            aux[i] = par[i].split(":")[1];

        try {
            LocalDateTime.parse(aux[0], DATEFORMAT);
            Integer.parseInt(aux[1]);
            Integer.parseInt(aux[2]);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public boolean checkCorredores(List<Corredor> corredores, int so, int sd) {
        for(Corredor c : corredores)
            if(c.corredorIgual(so, sd))
             return true;
        return false;
    }

    public boolean checkMedicao(String str) {
        if (checkErrorValue(str)) {
            return false;
        } else 
            return(checkCorredores(CORREDORES ,Integer.parseInt(str.split(",")[1].split(":")[1]), Integer.parseInt(str.split(",")[2].split(":")[1])));
    }

    public static void main(String[] args) {
    }

    public Alerta createAlertaAvaria(String datetime, String sala) {
        return new Alerta(datetime, sala, "", "", "Avaria", "Ocorreu um erro de escrita desta medição.");
    }

    public Alerta createAlertaIntermedio(String datetime, String sala, int ratos) {
        return new Alerta(datetime, sala, "", "", "Ratos Intermédio", "A sala" + sala + " tem " + ratos + " neste momento.");
    }

    public Alerta createAlertaCritico(String datetime, String sala) {
        return new Alerta(datetime, sala, "", "", "Ratos Crítico", "A sala " + sala + " excedeu os limites, abrir as Portas.");
    }

}
