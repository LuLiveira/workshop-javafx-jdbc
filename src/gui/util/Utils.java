package gui.util;

import javafx.event.ActionEvent;
import javafx.stage.Stage;
import javafx.scene.Node;

public class Utils {

	public static Stage currentStage(ActionEvent event) {
		return (Stage) ((Node) event.getSource()).getScene().getWindow();
	}

	public static Integer tryParseToInt(String s) {
		try {
			return Integer.parseInt(s);
		} catch (NumberFormatException e) {

			return null;
		}
	}

}
