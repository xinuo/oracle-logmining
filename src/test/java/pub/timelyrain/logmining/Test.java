package pub.timelyrain.logmining;

public class Test {
    public static void main(String[] args) {
        try{
            m1();
        }catch (Exception e){
            System.out.println("catch exception");
        }
    }

    private static void m1() throws Exception {

        try {
            System.out.println("try");
            throw new Exception("33");

        } finally {
            System.out.println("run finally");
        }
    }
}
