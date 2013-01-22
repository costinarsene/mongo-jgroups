package org.prodinf.jgroups.swing;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.WindowStateListener;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.RspList;

/**
 *
 * @author acostin
 */
public class ShowFrame extends JFrame implements MembershipListener {

  JButton button = null;
  JChannel channel;
  MessageDispatcher dispatcher;
  JTextArea messageWindow = null;
  JList membersList = null;

  public ShowFrame() throws Exception {
    channel = new JChannel(Test.class.getClassLoader().getResourceAsStream("org/prodinf/jgroups/swing/mongoping.xml"));

    channel.connect("ERP");
    dispatcher = new MessageDispatcher(channel, null, this, new RequestHandler() {
      @Override
      public Object handle(Message msg) throws Exception {
        messageWindow.setText("Processing message for:"+msg.getSrc());
        return "HELLO:" + System.currentTimeMillis();
      }
    });
	
	
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        ShowFrame.this.dispatcher.stop();
        ShowFrame.this.channel.close();
      }
    }));
    button = new JButton("Send message");
    button.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        Message msg = new Message();
        msg.setObject("Say hello");
        RspList rspList;
        try {
          rspList = dispatcher.castMessage(null, msg, RequestOptions.SYNC());
          messageWindow.setText("results:" + rspList.getResults());
        } catch (Exception ex) {
          ex.printStackTrace();
        }

      }
    });
    messageWindow = new JTextArea();
    messageWindow.setEditable(false);
    this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    membersList = new JList();
    System.out.println("Identified members:" + channel.getView().getMembers());
    extractMembers(channel.getView().getMembers());
    
  
    
    this.getContentPane().add(new JScrollPane(membersList), BorderLayout.EAST);
    this.getContentPane().add(new JScrollPane(messageWindow), BorderLayout.CENTER);
    this.getContentPane().add(button, BorderLayout.SOUTH);
  }

  private void extractMembers(java.util.List<Address> addresses) {

    DefaultListModel model = new DefaultListModel();
    for (Address address : addresses) {
      model.addElement( address.toString());
    }
    membersList.setModel(model);
  }

  @Override
  public void viewAccepted(final View new_view) {
    SwingUtilities.invokeLater(new Runnable() {
      @Override
      public void run() {
        extractMembers(new_view.getMembers());
      }
    });
  }

  @Override
  public void suspect(Address suspected_mbr) {
  }

  @Override
  public void block() {
  }

  @Override
  public void unblock() {
  }
}
