<?php

/**
 * CodeIgniter
 *
 * An open source application development framework for PHP
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2014 - 2019, British Columbia Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @package	CodeIgniter
 * @author	Muhammad Afif Anshor Libs
 * @copyright	Copyright (c) 2008 - 2014, EllisLab, Inc. (https://ellislab.com/)
 * @copyright	Copyright (c) 2014 - 2019, British Columbia Institute of Technology (https://bcit.ca/)
 * @license	https://opensource.org/licenses/MIT	MIT License
 * @link	https://codeigniter.com
 * @since	Version 1.0.0
 * @filesource
 */
defined('BASEPATH') or exit('No direct script access allowed');

/**
 * Query Builder Class
 *
 * This is the platform-independent base Query Builder implementation class.
 *
 * @package		CodeIgniter
 * @subpackage	Drivers
 * @category	Database
 * @author		EllisLab Dev Team
 * @link		https://codeigniter.com/user_guide/database/
 */

abstract class MY_DB_query_builder extends CI_DB_query_builder {

  // --------------------------------------------------------------------

  /**
   * Update_Batch
   *
   * Compiles an update string and runs the query
   *
   * @param	string	the table to retrieve the results from
   * @param	array	an associative array of update values
   * @param	string	the where key
   * @return	int	number of rows affected or FALSE on failure
   */
  function update_or_insert_batch($table = '', $set = NULL, $index = NULL, $identifier = true, $escape = NULL, $batch_size = 100) {
    $this->_merge_cache();
    if ($set === NULL) {
      if (empty($this->qb_set)) {
        return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
      }
    } else {
      if (empty($set)) {
        return ($this->db_debug) ? $this->display_error('insert_batch() called with no data') : FALSE;
      }

      $this->set_insert_batch($set, '', $escape);
    }

    if (strlen($table) === 0) {
      if (!isset($this->qb_from[0])) {
        return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
      }

      $table = $this->qb_from[0];
    }

    // Batch this baby
    $affected_rows = 0;
    $sql[]='';
    // Batch this baby
    for ($i = 0, $total = count($this->qb_set); $i < $total; $i += $batch_size) {
      $sql[$i] = $this->_insert_batch($this->protect_identifiers($table, TRUE, $escape, FALSE), $this->qb_keys, array_slice($this->qb_set, $i, $batch_size));
    }

    $this->_reset_write();
    if ($index === NULL) {
      return ($this->db_debug) ? $this->display_error('db_must_use_index') : FALSE;
    }

    if ($set === NULL) {
      if (empty($this->qb_set_ub)) {
        return ($this->db_debug) ? $this->display_error('db_must_use_set') : FALSE;
      }
    } else {
      if (empty($set)) {
        return ($this->db_debug) ? $this->display_error('update_batch() called with no data') : FALSE;
      }

      $this->set_update_batch($set, $index);
    }

    if (strlen($table) === 0) {
      if (!isset($this->qb_from[0])) {
        return ($this->db_debug) ? $this->display_error('db_must_set_table') : FALSE;
      }

      $table = $this->qb_from[0];
    }
    for ($i = 0, $total = count($this->qb_set_ub); $i < $total; $i += $batch_size) {
      $sql_update = $this->_on_duplicate_update_batch($this->protect_identifiers($table, TRUE, $escape, FALSE), array_slice($this->qb_set_ub, $i, $batch_size), $index);

      $sql[$i] = $sql[$i]
        . ' ON DUPLICATE KEY  '
        . $sql_update;
        $this->query($sql[$i]);
    }
    
    $this->_reset_write();
  }
      // --------------------------------------------------------------------

  /**
   * Update_or_Insert_Batch statement
   *
   * Generates a platform-specific batch update string from the supplied data
   *
   * @param	string	$table	Table name
   * @param	array	$values	Update data
   * @param	string	$index	WHERE key
   * @return	string
   */
  protected function _on_duplicate_update_batch($table, $values, $index) {
    
    $ids = array();
    foreach ($values as $key => $val) {
      $ids[] = $val[$index]['value'];

      foreach (array_keys($val) as $field) {
        if ($field !== $index) {
          $final[$val[$field]['field']][] = 'WHEN ' . $val[$index]['field'] . ' = ' . $val[$index]['value'] . ' THEN ' . $val[$field]['value'];
        }
      }
    }

    $cases = '';
    foreach ($final as $k => $v) {
      $cases .= $k . " = CASE \n"
        . implode("\n", $v) . "\n"
        . 'ELSE ' . $k . ' END, ';
    }

    $this->where($val[$index]['field'] . ' IN(' . implode(',', $ids) . ')', NULL, FALSE);

    return 'UPDATE ' .  '  ' . substr($cases, 0, -2);
  }
    /**
   * Insert batch statement
   *
   * Generates a platform-specific insert string from the supplied data.
   *
   * @param	string	$table	Table name
   * @param	array	$keys	INSERT keys
   * @param	array	$values	INSERT values
   * @return	string
   */
  protected function _insert_batch($table, $keys, $values) {
    return 'INSERT INTO ' . $table . ' (' . implode(', ', $keys) . ') VALUES ' . implode(', ', $values);
  }
}
