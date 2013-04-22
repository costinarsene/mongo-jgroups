package org.prodinf.jgroups.swing;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.Toolkit;
import javax.swing.JFrame;
import org.jgroups.conf.ClassConfigurator;

/**
 *
 * @author acostin
 */
public class Test {

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) throws Exception {
	//add to view 
    ClassConfigurator.addProtocol((short) (Short.MAX_VALUE - 1), org.prodinf.jgroups.MongoPing.class);
    //System.out.println("Am adaugat un protocol1");

    JFrame window = new ShowFrame();
    window.setSize(new Dimension(300, 400));
    Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
    // Determine the new location of the window
    int w = window.getSize().width;
    int h = window.getSize().height;
    int x = (dim.width - w) / 2;
    int y = (dim.height - h) / 2;
    window.setLocation(new Point(x, y)); 
    window.setVisible(true);


  }
}
