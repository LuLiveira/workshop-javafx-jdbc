package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmenteDaoJDBC implements DepartmentDao {

	private Connection connection;

	public DepartmenteDaoJDBC(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void insert(Department obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(Department obj) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub

	}

	@Override
	public Department findById(Integer id) {

		return null;
	}

	// Método auxiliar
	private Department instantiateSeller(ResultSet resultSet, Department department) throws SQLException {
		Department seller = new Department();
		seller.setId(resultSet.getInt("Id"));
		seller.setName(resultSet.getString("Name"));
		return seller;
	}

	// Método auxiliar
	private Department instantiateDepartment(ResultSet resultSet) throws SQLException {
		Department department = new Department();
		department.setId(resultSet.getInt("Id"));
		department.setName(resultSet.getString("Name"));
		return department;
	}

	@Override
	public List<Department> findAll() {

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		String query = "SELECT department.* FROM department";

		try {

			preparedStatement = connection.prepareStatement(query);
			resultSet = preparedStatement.executeQuery();

			List<Department> list = new ArrayList<Department>();
			// Map<Integer, Department> map = new HashMap<Integer, Department>();

			while (resultSet.next()) {

				Department department = instantiateDepartment(resultSet);
				list.add(department);
			}
			return list;

		} catch (SQLException e) {

			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(preparedStatement);
		}

	}

}
