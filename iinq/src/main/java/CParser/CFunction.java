package CParser;

public class CFunction {
    private String name;
    private String body;

    public CFunction(int funcCount, String body) {
        this.name = "SQLfunc" + funcCount;
        this.body = body;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getName() {
        return name;
    }
}
