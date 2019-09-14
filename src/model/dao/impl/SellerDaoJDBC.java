package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

	private Connection connection;

	public SellerDaoJDBC(Connection connection) {
		this.connection = connection;
	}

	@Override
	public void insert(Seller obj) {
		String query = "INSERT INTO seller (Name, Email, BirthDate, BaseSalary, DepartmentId) VALUES (?, ?, ?, ?, ?)";

		PreparedStatement preparedStatement = null;
		try {

			preparedStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

			preparedStatement.setString(1, obj.getName());
			preparedStatement.setString(2, obj.getEmail());
			preparedStatement.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			preparedStatement.setDouble(4, obj.getBaseSalary());
			preparedStatement.setInt(5, obj.getDepartment().getId());

			int rowsAffected = preparedStatement.executeUpdate();

			if (rowsAffected > 0) {

				ResultSet resultSet = preparedStatement.getGeneratedKeys();

				if (resultSet.next()) {
					int int1 = resultSet.getInt(1);
					obj.setId(1);
				}
				DB.closeResultSet(resultSet);
			} else {
				throw new DbException("Unexpected error! No rows affected.");
			}

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {

			DB.closeStatement(preparedStatement);
		}

	}

	@Override
	public void update(Seller obj) {

		String query = "UPDATE seller SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? WHERE Id = ?";
		PreparedStatement preparedStatement = null;

		try {

			preparedStatement = connection.prepareStatement(query);

			preparedStatement.setString(1, obj.getName());
			preparedStatement.setString(2, obj.getEmail());
			preparedStatement.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			preparedStatement.setDouble(4, obj.getBaseSalary());
			preparedStatement.setInt(5, obj.getDepartment().getId());
			preparedStatement.setInt(6, obj.getId());

			int update = preparedStatement.executeUpdate();

		} catch (SQLException e) {

			throw new DbException(e.getMessage());
		} finally {

			DB.closeStatement(preparedStatement);
		}

	}

	@Override
	public void deleteById(Integer id) {

		String query = "DELETE FROM seller WHERE Id = ?";
		PreparedStatement preparedStatement = null;

		try {

			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, id);
			int linhasafetadas = preparedStatement.executeUpdate();			
			

		} catch (SQLException e) {

			throw new DbException(e.getMessage());

		} finally {
			DB.closeStatement(preparedStatement);

		}

	}

	@Override
	public Seller findById(Integer id) {

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		String query = "SELECT seller.*, department.Name as DepName " + "FROM seller INNER JOIN department "
				+ "ON seller.DepartmentId = department.Id WHERE seller.Id = ?";
		try {
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, id);
			resultSet = preparedStatement.executeQuery();

			if (resultSet.next()) {
				Department department = instantiateDepartment(resultSet);
				Seller seller = instantiateSeller(resultSet, department);
				return seller;
			}
			return null;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(preparedStatement);
			DB.closeResultSet(resultSet);
		}
	}

	// M�todo auxiliar
	private Seller instantiateSeller(ResultSet resultSet, Department department) throws SQLException {
		Seller seller = new Seller();
		seller.setId(resultSet.getInt("Id"));
		seller.setName(resultSet.getString("Name"));
		seller.setEmail(resultSet.getString("Email"));
		seller.setBirthDate(new java.util.Date(resultSet.getTimestamp("BirthDate").getTime()));
		seller.setBaseSalary(resultSet.getDouble("BaseSalary"));
		seller.setDepartment(department);
		return seller;
	}

	// M�todo auxiliar
	private Department instantiateDepartment(ResultSet resultSet) throws SQLException {
		Department department = new Department();
		department.setId(resultSet.getInt("DepartmentId"));
		department.setName(resultSet.getString("DepName"));
		return department;
	}

	@Override
	public List<Seller> findAll() {

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		String query = "SELECT seller.*, department.Name as DepName FROM seller INNER JOIN department ON (seller.DepartmentId = department.Id) ORDER BY Name";

		try {

			preparedStatement = connection.prepareStatement(query);
			resultSet = preparedStatement.executeQuery();

			List<Seller> list = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<Integer, Department>();

			while (resultSet.next()) {

				Department department = map.get(resultSet.getInt("DepartmentId"));
				if (department == null) {
					department = instantiateDepartment(resultSet);
					map.put(resultSet.getInt("DepartmentId"), department);
				}
				Seller seller = instantiateSeller(resultSet, department);
				list.add(seller);
			}
			return list;

		} catch (SQLException e) {

			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(preparedStatement);
		}

	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		String query = "SELECT seller.*, department.Name as DepName FROM seller INNER JOIN department ON seller.DepartmentId = department.Id WHERE DepartmentId = ? ORDER BY Name";
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try {
			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, department.getId());
			resultSet = preparedStatement.executeQuery();

			List<Seller> list = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<>();

			while (resultSet.next()) {

				Department dep = map.get(resultSet.getInt("DepartmentId"));

				if (dep == null) {
					dep = instantiateDepartment(resultSet);
					map.put(resultSet.getInt("DepartmentId"), dep);
				}

				Seller seller2 = instantiateSeller(resultSet, dep);

				list.add(seller2);
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
